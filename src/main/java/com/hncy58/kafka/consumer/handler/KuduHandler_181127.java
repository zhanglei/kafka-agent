package com.hncy58.kafka.consumer.handler;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;
import com.hncy58.ds.ServerStatusReportUtil;
import com.hncy58.util.Utils;

/**
 * 写入数据至Kudu表处理器
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年11月6日 下午5:48:34
 *
 */
public class KuduHandler_181127 implements Handler {

	private static final Logger log = LoggerFactory.getLogger(KuduHandler_181127.class);
	private SessionConfiguration.FlushMode FLUSH_MODE = SessionConfiguration.FlushMode.MANUAL_FLUSH;
	private final static int OPERATION_BATCH = 1000000;

	private String agentSvrName;
	private String agentSvrGroup;
	private int agentSvrType;

	private String kuduMaster = "localhsot:7051";
	private String localFileNamePrefix = "unHadledData";
	private String kuduTablePrefix = "impala::kudu_";
	private String delStatusColName = "bigdata_del_status";
	private String syncTimeColname = "bigdata_sync_time";

	private static KuduClient client;

	private Map<String, Schema> kuduTableSchemas = new HashMap<>();

	public KuduHandler_181127(String agentSvrName, String agentSvrGroup, int agentSvrType, String kuduMaster,
			String localFileNamePrefix, String tblPrefix) throws Exception {
		super();
		this.agentSvrName = agentSvrName;
		this.agentSvrGroup = agentSvrGroup;
		this.agentSvrType = agentSvrType;
		this.kuduMaster = kuduMaster;
		this.localFileNamePrefix = localFileNamePrefix;
		this.kuduTablePrefix = tblPrefix;

		init();
	}

	/**
	 * 初始化
	 * 
	 * @throws Exception
	 */
	private void init() throws Exception {

		client = new KuduClient.KuduClientBuilder(kuduMaster).build();
		client.listTabletServers().getTabletServersList().forEach(server -> {
			log.info("kudu cluster server node -> " + server);
		});

		initKuduTables();
	}

	/**
	 * 初始化待同步表结构
	 * 
	 * @throws Exception
	 */
	private void initKuduTables() throws Exception {
		List<String> tables = client.getTablesList().getTablesList();
		for (String tableName : tables) {
			log.error("start to load {}'s schema.", tableName);
			Schema kuduSchema = client.openTable(tableName).getSchema();
			kuduTableSchemas.put(tableName, kuduSchema);
		}
	}

	@Override
	public boolean handle(List<ConsumerRecord<String, String>> data) throws Exception {

		if (data == null || data.isEmpty())
			return true;

		Map<String, KuduTable> kudutables = new HashMap<>();
		log.info("start init kudu session.");
		KuduSession session = client.newSession();
		session.setFlushMode(FLUSH_MODE);
		session.setMutationBufferSpace(OPERATION_BATCH);
		log.info("init kudu session finished.");
		Map<String, List<Upsert>> upsertsMap = new HashMap<>();
		Map<String, List<Delete>> deletesMap = new HashMap<>();
		Schema kuduSchema;
		String tblId;
		String dbId;
		long syncTime;
		JSONObject schema;
		JSONArray jsonDataArr;
		JSONObject json = null;
		String value;
		String oprType;
		String pkCols;
		ArrayList<String> rowKeys = new ArrayList<>();
		KuduTable table;
		ColumnSchema colSchema = null;
		Set<String> unExistTable = new HashSet<>();
		Set<Type> ignoredTypes = new HashSet<>(Arrays.asList(Type.DOUBLE, Type.FLOAT, Type.INT8, Type.INT16, Type.INT32,
				Type.INT64, Type.BOOL, Type.UNIXTIME_MICROS, Type.DECIMAL));

		log.info("start parse kafka data.");
		long start = System.currentTimeMillis();
		long tmpCnt = 0L;
		
		for (ConsumerRecord<String, String> r : data) {
			value = r.value();
			if (StringUtils.isEmpty(value)) {
				log.warn("data value is null, ignored, record:{}", r);
				continue;
			}

			try {
				json = JSONObject.parseObject(value);
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}

			if (json == null || json.isEmpty() || !json.containsKey("schema") || !json.containsKey("data")) {
				log.warn("json value is null or empty, not contain schema,not contain data, ignored, record:{}", r);
				continue;
			}

			schema = json.getJSONObject("schema");
			jsonDataArr = json.getJSONArray("data");
			if (jsonDataArr.isEmpty()) {
				log.warn("json value data field is null, ignored, record:{}", r);
				continue;
			}

			tblId = schema.getString("tbl_id");
			dbId = schema.getString("db_id");
			syncTime = schema.getLong("time");
			tblId = Utils.isEmpty(dbId) ? tblId : kuduTablePrefix + dbId + "." + tblId;

			if (!kuduTableSchemas.containsKey(tblId)) {
				unExistTable.add(tblId);
				log.warn("Kudu表:{}对应Schema不存在或者未加载成功，数据被忽略 ,data:\n{}", tblId, value);
				continue;
			}

			kuduSchema = kuduTableSchemas.get(tblId);

			log.debug("start init kudu table.");
			if (kudutables.containsKey(tblId)) {
				table = kudutables.get(tblId);
			} else {
				log.info("add new kudu table instance to kudutables cache.");
				table = client.openTable(tblId);
				kudutables.put(tblId, table);
			}
			log.debug("init kudu table finished.");
			oprType = schema.getString("opr_type");
			pkCols = schema.getString("pk_col");

			if (pkCols != null && !"".equals(pkCols.trim())) {
				rowKeys.clear();
				rowKeys.addAll(Arrays.asList(pkCols.split(" *, *")));
			}

			log.debug("start parse list data.");
			long parseListStart = System.currentTimeMillis();
			if ("i".equals(oprType) || "u".equals(oprType)) {
				List<Upsert> listUpsert = new ArrayList<Upsert>();
				for (Object dataObj : jsonDataArr) {
					JSONObject dataJson = null;
					if (dataObj instanceof JSONObject) {
						dataJson = (JSONObject) dataObj;
					} else {
						log.warn("data child is not correct json :{}", dataObj);
						continue;
					}

					StringBuffer idBuf = new StringBuffer("");
					for (String id : rowKeys) {
						idBuf.append("_" + dataJson.getString(id));
					}

					if (idBuf.length() > 0) {
						Upsert upsert = table.newUpsert();
						PartialRow row = upsert.getRow();

						// 判断是否含同步时间状态字段
						try {
							row.addLong(syncTimeColname, syncTime);
						} catch (Exception e) {
							log.debug(tblId + "表没有同步时间字段," + e.getMessage(), e);
						}

						for (Entry<String, Object> entry : dataJson.entrySet()) {
							try {
								colSchema = kuduSchema.getColumn(entry.getKey().toLowerCase());
							} catch (Exception e) {
								log.warn(tblId + "表没有字段:" + entry.getKey() + "," + e.getMessage(), e);
								continue;
							}
							// if (entry.getValue() == null) {
							// row.setNull(entry.getKey().toLowerCase());
							// continue;
							// }
							if (entry.getValue() == null || (Utils.isEmpty(entry.getValue())
									&& ignoredTypes.contains(colSchema.getType()))) {
								row.setNull(entry.getKey().toLowerCase());
								continue;
							}

							switch (colSchema.getType()) {
							case BINARY:
								row.addBinary(entry.getKey().toLowerCase(), TypeUtils.castToBytes(entry.getValue()));
								break;
							case BOOL:
								row.addBoolean(entry.getKey().toLowerCase(), TypeUtils.castToBoolean(entry.getValue()));
								break;
							case DOUBLE:
								row.addDouble(entry.getKey().toLowerCase(), TypeUtils.castToDouble(entry.getValue()));
								break;
							case FLOAT:
								row.addFloat(entry.getKey().toLowerCase(), TypeUtils.castToFloat(entry.getValue()));
								break;
							case INT8:
								row.addByte(entry.getKey().toLowerCase(), TypeUtils.castToByte(entry.getValue()));
								break;
							case INT16:
								row.addInt(entry.getKey().toLowerCase(), TypeUtils.castToInt(entry.getValue()));
								break;
							case INT32:
								row.addInt(entry.getKey().toLowerCase(), TypeUtils.castToInt(entry.getValue()));
								break;
							case INT64:
								row.addLong(entry.getKey().toLowerCase(), castToLong(entry.getValue()));
								break;
							case STRING:
								row.addString(entry.getKey().toLowerCase(), TypeUtils.castToString(entry.getValue()));
								break;
							case UNIXTIME_MICROS:
								row.addLong(entry.getKey().toLowerCase(), castToLong(entry.getValue()));
								break;
							default:
								break;
							}
						}

						listUpsert.add(upsert);
						++ tmpCnt;
					}
				}

				if (upsertsMap.containsKey(tblId)) {
					upsertsMap.get(tblId).addAll(listUpsert);
				} else {
					upsertsMap.put(tblId, listUpsert);
				}
			} else if ("d".equals(oprType)) {
				List<Delete> listDelete = new ArrayList<Delete>();
				for (Object dataObj : jsonDataArr) {
					if (dataObj != null && dataObj instanceof JSONObject) {
					} else {
						continue;
					}

					JSONObject rowJson = (JSONObject) dataObj;
					Delete delete = table.newDelete();
					PartialRow row = delete.getRow();

					// 判断是否含同步时间状态字段
					try {
						row.addInt(delStatusColName, 1);
						row.addLong(syncTimeColname, syncTime);
					} catch (Exception e) {
						log.debug(tblId + "表没有同步时间、删除状态字段," + e.getMessage(), e);
					}

					for (String key : rowKeys) {
						try {
							colSchema = kuduSchema.getColumn(key.toLowerCase());
						} catch (Exception e) {
							log.warn(tblId + "表没有字段:" + key + "," + e.getMessage(), e);
							continue;
						}
						
						switch (colSchema.getType()) {
						case BINARY:
							row.addBinary(key.toLowerCase(), rowJson.getBytes(key));
							break;
						case BOOL:
							row.addBoolean(key.toLowerCase(), rowJson.getBoolean(key));
							break;
						case DOUBLE:
							row.addDouble(key.toLowerCase(), rowJson.getDouble(key));
							break;
						case FLOAT:
							row.addFloat(key.toLowerCase(), rowJson.getFloat(key));
							break;
						case INT8:
							row.addByte(key.toLowerCase(), rowJson.getByte(key));
							break;
						case INT16:
							row.addInt(key.toLowerCase(), rowJson.getInteger(key));
							break;
						case INT32:
							row.addInt(key.toLowerCase(), rowJson.getInteger(key));
							break;
						case INT64:
							row.addLong(key.toLowerCase(), castToLong(rowJson.get(key)));
							break;
						case DECIMAL:
							row.addDecimal(key.toLowerCase(), castToDecimal(rowJson.get(key), colSchema.getTypeAttributes()));
							break;
						case STRING:
							row.addString(key.toLowerCase(), rowJson.getString(key));
							break;
						case UNIXTIME_MICROS:
							row.addLong(key.toLowerCase(), castToLong(rowJson.get(key)));
							break;
						default:
							break;
						}
					}

					listDelete.add(delete);
					++ tmpCnt;
				}

				if (deletesMap.containsKey(tblId)) {
					deletesMap.get(tblId).addAll(listDelete);
				} else {
					deletesMap.put(tblId, listDelete);
				}
			}
			log.debug("parse list data finished, used {} ms.", System.currentTimeMillis() - parseListStart);
		}

		if (!unExistTable.isEmpty()) {
			boolean ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
					"Kudu表不存在或者未加载成功，数据被忽略，tableList:" + unExistTable);
		}

		log.error("parse kafka data finished size:{}, used {} ms.", tmpCnt, System.currentTimeMillis() - start);
		doCommit(session, upsertsMap, deletesMap);

		return true;
	}

	private BigDecimal castToDecimal(Object data, ColumnTypeAttributes typeAttributes) {

		if (data instanceof Number)
			return TypeUtils.castToBigDecimal(data);
		
		if (data == null || "".equals(data.toString().trim())) {
			return null;
		}
		
		return new BigDecimal(data.toString().trim());
//		return TypeUtils.castToBigDecimal(data);
	}

	private static Long castToLong(Object date) {

		if (date instanceof Number)
			return TypeUtils.castToLong(date);

		if (date == null || "".equals(date.toString().trim())) {
			return null;
		}
		String dateStr = date.toString().trim();
		if (dateStr.length() > 19) {
			dateStr = dateStr.substring(0, 19);
		}
		return TypeUtils.castToLong(dateStr);
	}

	/**
	 * 提交数据到Kudu中
	 * 
	 * @param session
	 * @param upsertsMap
	 * @param deletesMap
	 * @throws KuduException
	 */
	private void doCommit(KuduSession session, Map<String, List<Upsert>> upsertsMap,
			Map<String, List<Delete>> deletesMap) throws KuduException {

		if (upsertsMap.isEmpty() && deletesMap.isEmpty())
			return;

		try {
			long start = System.currentTimeMillis();
			for (Entry<String, List<Upsert>> entry : upsertsMap.entrySet()) {
				int cnt = 0;
				log.error("start upsert table {} data, size -> {}", entry.getKey(), entry.getValue().size());
				for (Upsert upsert : entry.getValue()) {
					session.apply(upsert);
					if(cnt >= OPERATION_BATCH) {
						session.flush();
						cnt = 0;
					}
					cnt ++;
				}
			}
			log.error("commit upsert batch used {} ms.", System.currentTimeMillis() - start);

			start = System.currentTimeMillis();
			for (Entry<String, List<Delete>> entry : deletesMap.entrySet()) {
				int cnt = 0;
				log.error("start upsert table {} data, size -> {}", entry.getKey(), entry.getValue().size());
				for (Delete delete : entry.getValue()) {
					session.apply(delete);
					if(cnt >= OPERATION_BATCH) {
						session.flush();
						cnt = 0;
					}
					cnt ++;
				}
			}
			log.error("commit delete batch used {} ms.", System.currentTimeMillis() - start);

		} finally {
			session.flush();
			if (session != null && !session.isClosed()) {
				session.close();
			}
		}
	}

	@Override
	public void onHandleFail(List<ConsumerRecord<String, String>> data) throws Exception {

		if (data == null || data.isEmpty())
			return;

		long startOffset = data.get(0).offset();
		long endOffset = data.get(data.size() - 1).offset();
		Map<String, StringBuffer> buffMap = new HashMap<>();

		log.error("start to store datas to local -> " + data.size());
		data.forEach(record -> {
			String tmpStr = (record.timestamp() + "," + record.partition() + "," + record.offset() + "," + record.key()
					+ "," + record.value() + "\n");
			if (buffMap.containsKey(record.topic())) {
				buffMap.get(record.topic()).append(tmpStr);
			} else {
				StringBuffer buf = new StringBuffer();
				buf.append(tmpStr);
				buffMap.put(record.topic(), buf);
			}
		});

		if (!buffMap.isEmpty()) {
			for (Entry<String, StringBuffer> entry : buffMap.entrySet()) {
				String topic = entry.getKey();
				StringBuffer buf = entry.getValue();
				if (buf != null && buf.length() > 0) {
					BufferedOutputStream bos = null;
					try {
						String fileName = localFileNamePrefix + "_" + topic + "_"
								+ new SimpleDateFormat("yyyyMMddHH").format(new Date()) + "_" + startOffset + "-"
								+ endOffset;
						bos = new BufferedOutputStream(new FileOutputStream(fileName, true));
						IOUtils.copyBytes(new ByteArrayInputStream(buf.toString().getBytes("UTF-8")), bos, 4096, true);
						bos.flush();
					} finally {
						IOUtils.closeStream(bos);
					}
				}
			}
		}

		log.error("end stored datas to local.");
	}

	public String getKuduMaster() {
		return kuduMaster;
	}

	public void setKuduMaster(String kuduMaster) {
		this.kuduMaster = kuduMaster;
	}

	public String getLocalFileNamePrefix() {
		return localFileNamePrefix;
	}

	public void setLocalFileNamePrefix(String localFileNamePrefix) {
		this.localFileNamePrefix = localFileNamePrefix;
	}

	public static Logger getLog() {
		return log;
	}

	public static KuduClient getClient() {
		return client;
	}

}
