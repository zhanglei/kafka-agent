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
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
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
public class KuduHandler implements Handler {

	private static final Logger log = LoggerFactory.getLogger(KuduHandler.class);
	private SessionConfiguration.FlushMode FLUSH_MODE = SessionConfiguration.FlushMode.MANUAL_FLUSH;
	private final static int OPERATION_BATCH = 15000;
	private static final long defaultOperationTimeoutMs = 0;
	private static final long defaultSessionTimeoutMs = 600000;
	private static final long defaultSocketReadTimeoutMs = 600000;
	private static final long defaultAdminOperationTimeoutMs = 600000;

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

	public KuduHandler(String agentSvrName, String agentSvrGroup, int agentSvrType, String kuduMaster,
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

		client = new KuduClient.KuduClientBuilder(kuduMaster).defaultOperationTimeoutMs(defaultOperationTimeoutMs)
				.defaultSocketReadTimeoutMs(defaultSocketReadTimeoutMs)
				.defaultAdminOperationTimeoutMs(defaultAdminOperationTimeoutMs).build();

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
			log.error("load {}'s schema finished. kuduSchema -> {}", tableName, kuduSchema);
		}
	}

	@Override
	public boolean handle(List<ConsumerRecord<String, String>> data) throws Exception {

		if (data == null || data.isEmpty())
			return true;

		Map<String, KuduTable> kudutables = new HashMap<>();
		Map<String, List<Operation>> upsertMap = new HashMap<>();
		Map<String, List<Operation>> insertMap = new HashMap<>();
		Map<String, List<Operation>> deleteMap = new HashMap<>();
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

		log.info("start parse kafka data.");
		long start = System.currentTimeMillis();
		long tmpCnt = 0L;

		for (ConsumerRecord<String, String> r : data) {
			value = r.value();
			// for debug
			log.info("received data -> {}", value);
			
			if (StringUtils.isEmpty(value)) {
				log.error("data value is null, ignored, record:{}", r);
				continue;
			}

			try {
				json = JSONObject.parseObject(value);
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}

			if (json == null || json.isEmpty() || !json.containsKey("schema") || !json.containsKey("data")) {
				log.error("json value is null or empty, not contain schema,not contain data, ignored, record:{}", r);
				continue;
			}

			schema = json.getJSONObject("schema");
			jsonDataArr = json.getJSONArray("data");
			if (jsonDataArr.isEmpty()) {
				log.error("json value data field is null, ignored, record:{}", r);
				continue;
			}

			tblId = schema.getString("tbl_id");
			dbId = schema.getString("db_id");
			syncTime = schema.getLong("time");
			tblId = Utils.isEmpty(dbId) ? tblId : kuduTablePrefix + dbId + "." + tblId;

			if (!kuduTableSchemas.containsKey(tblId)) {
				unExistTable.add(tblId);
//				log.debug("Kudu表:{}对应Schema不存在或者未加载成功，数据被忽略 ,data:\n{}", tblId, value);
				log.warn("Kudu表:{}对应Schema不存在或者未加载成功，数据被忽略", tblId);
				continue;
			}

			kuduSchema = kuduTableSchemas.get(tblId);

			log.debug("start init kudu table.");
			if (kudutables.containsKey(tblId)) {
				table = kudutables.get(tblId);
			} else {
				log.info("add new kudu {} table instance to kudutables cache.", tblId);
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
			List<Operation> insertList = new ArrayList<Operation>();
			List<Operation> upsertList = new ArrayList<Operation>();
			List<Operation> deleteList = new ArrayList<Operation>();
			for (Object dataObj : jsonDataArr) {
				JSONObject dataJson = null;
				if (dataObj instanceof JSONObject) {
					dataJson = (JSONObject) dataObj;
				} else {
					log.error("data child is not correct json :{}", dataObj);
					continue;
				}

				Operation option = null;
				PartialRow row = null;
				int delStatus = -1;

				switch (oprType) {
				case "i":
					delStatus = 0;
					option = table.newUpsert();
					insertList.add(option);
					break;
				case "u":
					delStatus = 2;
					option = table.newUpsert();
					upsertList.add(option);
					break;
				case "d":
					option = table.newUpsert();
					delStatus = 1;
					deleteList.add(option);
					break;
				default:
					log.error("not correct oprType:{}", oprType);
					break;
				}
				
				row = option.getRow();

				try {
					// 判断是否含同步时间、删除状态字段
					row.addInt(delStatusColName, delStatus);
					row.addLong(syncTimeColname, syncTime);
				} catch (Exception e) {
					log.error(tblId + "表没有同步时间、删除状态字段," + e.getMessage(), e);
				}

				for (Entry<String, Object> entry : dataJson.entrySet()) {
					try {
						colSchema = kuduSchema.getColumn(entry.getKey().toLowerCase());
					} catch (Exception e) {
						log.debug(tblId + "表没有字段:" + entry.getKey() + "," + e.getMessage());
						continue;
					}
					if (entry.getValue() == null) {
						row.setNull(entry.getKey().toLowerCase());
						continue;
					}

					fillRow(colSchema, row, entry);
				}

				++tmpCnt;
			}

			if (!insertList.isEmpty()) {
				if (insertMap.containsKey(tblId)) {
					insertMap.get(tblId).addAll(insertList);
				} else {
					insertMap.put(tblId, insertList);
				}
			}

			if (!upsertList.isEmpty()) {
				if (upsertMap.containsKey(tblId)) {
					upsertMap.get(tblId).addAll(upsertList);
				} else {
					upsertMap.put(tblId, upsertList);
				}
			}

			if (!deleteList.isEmpty()) {
				if (deleteMap.containsKey(tblId)) {
					deleteMap.get(tblId).addAll(deleteList);
				} else {
					deleteMap.put(tblId, deleteList);
				}
			}
			log.debug("parse list data finished, used {} ms.", System.currentTimeMillis() - parseListStart);
		}

		if (!unExistTable.isEmpty()) {
			ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
					"Kudu表不存在或者未加载成功，数据被忽略，tableList:" + unExistTable);
		}

		log.error("parse kafka data finished size:{}, used {} ms.", tmpCnt, System.currentTimeMillis() - start);

		
		doCommit(insertMap, upsertMap, deleteMap);

		return true;
	}

	private void fillRow(ColumnSchema colSchema, PartialRow row, Entry<String, Object> entry) {
		
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
		case DECIMAL: // kudu-1.7.0以后版本支持此数据格式
			row.addDecimal(entry.getKey().toLowerCase(),
					castToDecimal(entry.getValue(), colSchema.getTypeAttributes()));
			break;
		case STRING:
			row.addString(entry.getKey().toLowerCase(), TypeUtils.castToString(entry.getValue()));
			break;
		case UNIXTIME_MICROS:
			row.addLong(entry.getKey().toLowerCase(), castToLong(entry.getValue()));
			break;
		default:
			break;
		}}

	private BigDecimal castToDecimal(Object data, ColumnTypeAttributes typeAttributes) {

		if (data instanceof Number)
			return TypeUtils.castToBigDecimal(data);

		if (data == null || "".equals(data.toString().trim())) {
			return null;
		}

		return new BigDecimal(data.toString().trim());
		// return TypeUtils.castToBigDecimal(data);
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
	 * @param insertMap
	 * @param upsertMap
	 * @param deleteMap
	 * @throws KuduException
	 */
	private void doCommit(Map<String, List<Operation>> insertMap,
			Map<String, List<Operation>> upsertMap, Map<String, List<Operation>> deleteMap) throws KuduException {

		log.info("start init kudu session.");
		KuduSession session = client.newSession();
		session.setFlushMode(FLUSH_MODE);
		session.setMutationBufferSpace(OPERATION_BATCH);
		session.setTimeoutMillis(defaultSessionTimeoutMs);
		log.info("init kudu session finished.");
		
		long start = 0;
		try {
			if (!insertMap.isEmpty()) {
				start = System.currentTimeMillis();
				int cnt = 0;
				for (Entry<String, List<Operation>> entry : insertMap.entrySet()) {
					log.error("start insert table {} data, size -> {}", entry.getKey(), entry.getValue().size());
					for (Operation option : entry.getValue()) {
						session.apply(option);
						if (cnt >= OPERATION_BATCH / 3) {
							List<OperationResponse> resps = session.flush();
							resps.forEach(resp -> {
								if(resp.hasRowError()) {
									log.error("error insert msg:{},{},{}" + resp.getRowError().getMessage(), resp.getRowError().getStatus(), resp.getRowError().getErrorStatus().toString());
									log.error("error insert row:{}" + resp.getRowError().getOperation().getRow().toString());
								}
							});
							cnt = 0;
						}
						cnt++;
					}
				}
				
				if(cnt > 0) {
					List<OperationResponse> resps = session.flush();
					resps.forEach(resp -> {
						if(resp.hasRowError()) {
							log.error("error insert msg:{},{},{}" + resp.getRowError().getMessage(), resp.getRowError().getStatus(), resp.getRowError().getErrorStatus().toString());
							log.error("error insert row:{}" + resp.getRowError().getOperation().getRow().toString());
						}
					});
					cnt = 0;
				}
				
				log.error("commit insert batch used {} ms.", System.currentTimeMillis() - start);
			}

			if (!upsertMap.isEmpty()) {
				start = System.currentTimeMillis();
				int cnt = 0;
				for (Entry<String, List<Operation>> entry : upsertMap.entrySet()) {
					log.error("start upsert table {} data, size -> {}", entry.getKey(), entry.getValue().size());
					for (Operation option : entry.getValue()) {
						session.apply(option);
						if (cnt >= OPERATION_BATCH / 3) {
							List<OperationResponse> resps = session.flush();
							resps.forEach(resp -> {
								if(resp.hasRowError()) {
									log.error("error upsert msg:{},{},{}" + resp.getRowError().getMessage(), resp.getRowError().getStatus(), resp.getRowError().getErrorStatus().toString());
									log.error("error upsert row:{}" + resp.getRowError().getOperation().getRow().toString());
								}
							});
							cnt = 0;
						}
						cnt++;
					}
				}
				
				if(cnt > 0) {
					List<OperationResponse> resps = session.flush();
					resps.forEach(resp -> {
						if(resp.hasRowError()) {
							log.error("error upsert msg:{},{},{}" + resp.getRowError().getMessage(), resp.getRowError().getStatus(), resp.getRowError().getErrorStatus().toString());
							log.error("error upsert row:{}" + resp.getRowError().getOperation().getRow().toString());
						}
					});
					cnt = 0;
				}
				log.error("commit upsert batch used {} ms.", System.currentTimeMillis() - start);
			}

			if (!deleteMap.isEmpty()) {
				start = System.currentTimeMillis();
				int cnt = 0;
				for (Entry<String, List<Operation>> entry : deleteMap.entrySet()) {
					log.error("start delete table {} data, size -> {}", entry.getKey(), entry.getValue().size());
					for (Operation option : entry.getValue()) {
						session.apply(option);
						if (cnt >= OPERATION_BATCH / 3) {
							List<OperationResponse> resps = session.flush();
							resps.forEach(resp -> {
								if(resp.hasRowError()) {
									log.error("error delete msg:{},{},{}" + resp.getRowError().getMessage(), resp.getRowError().getStatus(), resp.getRowError().getErrorStatus().toString());
									log.error("error delete row:{}" + resp.getRowError().getOperation().getRow().toString());
								}
							});
							cnt = 0;
						}
						cnt++;
					}
				}
				
				if(cnt > 0) {
					List<OperationResponse> resps = session.flush();
					resps.forEach(resp -> {
						if(resp.hasRowError()) {
							log.error("error delete msg:{},{},{}" + resp.getRowError().getMessage(), resp.getRowError().getStatus(), resp.getRowError().getErrorStatus().toString());
							log.error("error delete row:{}" + resp.getRowError().getOperation().getRow().toString());
						}
					});
					cnt = 0;
				}
				log.error("commit delete batch used {} ms.", System.currentTimeMillis() - start);
			}

		} finally {
			start = System.currentTimeMillis();
			session.flush();
			log.error("batch data all commited. finally flush opts used {} ms.", System.currentTimeMillis() - start);
			if (session != null && !session.isClosed()) {
				session.close();
				log.error("close session finished.");
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
