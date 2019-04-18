package com.hncy58.kafka.consumer.handler;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
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
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
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
import com.hncy58.ds.DSPoolUtil;
import com.hncy58.ds.ServerStatusReportUtil;
import com.hncy58.util.Utils;

/**
 * 写入数据至Kudu表处理器
 * @author	tokings
 * @company	hncy58	湖南长银五八
 * @website	http://www.hncy58.com
 * @version 1.0
 * @date	2018年11月6日 下午5:48:34
 *
 */
public class KuduHandler_v1 implements Handler {

	private static final Logger log = LoggerFactory.getLogger(KuduHandler_v1.class);
	private static final String QUERY_SQL = "SELECT * FROM sync_table_schema t where t.table_type = 1 and t.status = 1";
	private SessionConfiguration.FlushMode FLUSH_MODE = SessionConfiguration.FlushMode.MANUAL_FLUSH;
	private final static int OPERATION_BATCH = 10000;

	private String agentSvrName;
	private String agentSvrGroup;
	private int agentSvrType;

	private String kuduMaster = "localhsot:7051";
	private int numReplicas = 1;
	private int buckets = 16;
	private String localFileNamePrefix = "unHadledData";

	private static KuduClient client;

	private List<Map<String, Object>> tables = new ArrayList<>();
	private Map<String, Schema> kuduTableSchemas = new HashMap<>();

	public KuduHandler_v1(String agentSvrName, String agentSvrGroup, int agentSvrType, String kuduMaster, int numReplicas,
			int buckets, String localFileNamePrefix) throws Exception {
		super();
		this.agentSvrName = agentSvrName;
		this.agentSvrGroup = agentSvrGroup;
		this.agentSvrType = agentSvrType;
		this.kuduMaster = kuduMaster;
		this.numReplicas = numReplicas;
		this.buckets = buckets;
		this.localFileNamePrefix = localFileNamePrefix;

		init();
	}

	/**
	 *  初始化
	 * @throws Exception
	 */
	private void init() throws Exception {

		client = new KuduClient.KuduClientBuilder(kuduMaster).build();
		client.listTabletServers().getTabletServersList().forEach(server -> {
			System.out.println("kudu cluster server node -> " + server);
		});

		initKuduTables();
	}

	/**
	 * 初始化待同步表结构
	 * @throws Exception
	 */
	private void initKuduTables() throws Exception {
		tables = DSPoolUtil.query(QUERY_SQL, new Object[] {});

		for (Map<String, Object> tableMap : tables) {
			String dbName = Utils.toString(tableMap.get("db_id"));
			String tableName = Utils.toString(tableMap.get("table_id"));
			String schema = Utils.toString(tableMap.get("schema_json"));
			String realTableName = Utils.isEmpty(dbName) ? tableName : dbName + ":" + tableName;
			JSONArray schemaJson = JSONObject.parseArray(schema);
			List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
			List<String> hashKeys = new ArrayList<>();
			List<String> rangeKeys = new ArrayList<>();

			schemaJson.forEach(field -> {
				if (field instanceof JSONObject) {
					JSONObject fieldJson = (JSONObject) field;
					String name = fieldJson.getString("name");
					String type = fieldJson.getString("type");
					boolean nullable = fieldJson.getBooleanValue("nullable");

					if (fieldJson.containsKey("pkType")) {
						String pkType = fieldJson.getString("pkType");
						switch (pkType) {
						case "rangePartition":
							rangeKeys.add(name);
							break;
						case "hashPartition":
							hashKeys.add(name);
							break;
						default:
							break;
						}
						columns.add(new ColumnSchema.ColumnSchemaBuilder(name, Type.valueOf(type)).key(true).nullable(nullable).build());
					} else {
						columns.add(new ColumnSchema.ColumnSchemaBuilder(name, Type.valueOf(type)).build());
					}
				}
			});

			Schema kuduSchema = new Schema(columns);
			kuduTableSchemas.put(realTableName, kuduSchema);
			// 如果表不存在则创建表
			if (!client.tableExists(realTableName)) {
				CreateTableOptions createOptions = new CreateTableOptions().setNumReplicas(numReplicas);
				if (!rangeKeys.isEmpty()) {
					createOptions.setRangePartitionColumns(rangeKeys);
				}
				if (!hashKeys.isEmpty()) {
					createOptions.addHashPartitions(hashKeys, buckets);
				}
				client.createTable(realTableName, kuduSchema, createOptions);
			}
		}
	}

	@Override
	public boolean handle(List<ConsumerRecord<String, String>> data) throws Exception {

		if (data == null || data.isEmpty())
			return true;

		Map<String, KuduTable> kudutables = new HashMap<>();
		Set<String> existTables = new HashSet<>();
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
		JSONObject schema;
		JSONArray jsonDataArr;
		JSONObject json = null;
		String value;
		String oprType;
		String pkCols;
		ArrayList<String> rowKeys = new ArrayList<>();
		KuduTable table;
		
		log.info("start parse kafka data.");
		long start = System.currentTimeMillis();
		for (ConsumerRecord<String, String> r : data) {
			value = r.value();
			if (StringUtils.isEmpty(value)) {
				log.warn("data value is null, ignored, record:{}", r);
				continue;
			}

			try {
				json = JSONObject.parseObject(value);
			} catch (Exception e) {
				log.warn(e.getMessage(), e);
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
			tblId = Utils.isEmpty(dbId) ? tblId : dbId + ":" + tblId;

			// TODO 是否需要这一步？
			if(! existTables.contains(tblId)) {
				if (!client.tableExists(tblId)) {
					boolean ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
							"Kudu表：" + tblId + "不存在，数据被忽略，data:" + value);
					log.warn("Kudu表:{}不存在，数据被忽略,alarm:{} ,data:{}", tblId, ret, value);
					continue;
				}
				existTables.add(tblId);
				log.info("add new exists kudu table to existTables cache.");
			}

			kuduSchema = kuduTableSchemas.get(tblId);
			if(Utils.isEmpty(kuduSchema)) {
				boolean ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
						"Kudu表：" + tblId + "对应Schema不存在或者未加载成功，数据被忽略，data:" + value);
				log.warn("Kudu表:{}对应Schema不存在或者未加载成功，数据被忽略,alarm:{} ,data:{}", tblId, ret, value);
				continue;
			}
			
			log.debug("start init kudu table.");
			if(kudutables.containsKey(tblId)) {
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
				for(Object dataObj : jsonDataArr) {
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
						for(Entry<String, Object> entry : dataJson.entrySet()) {
							ColumnSchema colSchema = kuduSchema.getColumn(entry.getKey());
							if (colSchema == null)
								continue;
							switch (colSchema.getType()) {
							case BINARY:
								row.addBinary(entry.getKey(), TypeUtils.castToBytes(entry.getValue()));
								break;
							case BOOL:
								row.addBoolean(entry.getKey(), TypeUtils.castToBoolean(entry.getValue()));
								break;
							case DOUBLE:
								row.addDouble(entry.getKey(), TypeUtils.castToDouble(entry.getValue()));
								break;
							case FLOAT:
								row.addFloat(entry.getKey(), TypeUtils.castToFloat(entry.getValue()));
								break;
							case INT8:
								row.addByte(entry.getKey(), TypeUtils.castToByte(entry.getValue()));
								break;
							case INT16:
								row.addInt(entry.getKey(), TypeUtils.castToInt(entry.getValue()));
								break;
							case INT32:
								row.addInt(entry.getKey(), TypeUtils.castToInt(entry.getValue()));
								break;
							case INT64:
								row.addLong(entry.getKey(), TypeUtils.castToLong(entry.getValue()));
								break;
							case STRING:
								row.addString(entry.getKey(), TypeUtils.castToString(entry.getValue()));
								break;
							case UNIXTIME_MICROS:
								row.addLong(entry.getKey(), TypeUtils.castToLong(entry.getValue()));
								break;
							default:
								break;
							}
						}

						listUpsert.add(upsert);
					}
				}

				if (upsertsMap.containsKey(tblId)) {
					upsertsMap.get(tblId).addAll(listUpsert);
				} else {
					upsertsMap.put(tblId, listUpsert);
				}
			} else if ("d".equals(oprType)) {
				List<Delete> listDelete = new ArrayList<Delete>();
				for(Object dataObj : jsonDataArr) {
					if (dataObj != null && dataObj instanceof JSONObject) {
					} else {
						continue;
					}

					JSONObject rowJson = (JSONObject) dataObj;

					Delete delete = table.newDelete();
					PartialRow row = delete.getRow();
					for(String key : rowKeys) {
						ColumnSchema colSchema = kuduSchema.getColumn(key);
						if (colSchema == null)
							continue;
						switch (colSchema.getType()) {
						case BINARY:
							row.addBinary(key, rowJson.getBytes(key));
							break;
						case BOOL:
							row.addBoolean(key, rowJson.getBoolean(key));
							break;
						case DOUBLE:
							row.addDouble(key, rowJson.getDouble(key));
							break;
						case FLOAT:
							row.addFloat(key, rowJson.getFloat(key));
							break;
						case INT8:
							row.addByte(key, rowJson.getByte(key));
							break;
						case INT16:
							row.addInt(key, rowJson.getInteger(key));
							break;
						case INT32:
							row.addInt(key, rowJson.getInteger(key));
							break;
						case INT64:
							row.addLong(key, rowJson.getLong(key));
							break;
						case STRING:
							row.addString(key, rowJson.getString(key));
							break;
						case UNIXTIME_MICROS:
							row.addLong(key, rowJson.getLong(key));
							break;
						default:
							break;
						}
					}

					listDelete.add(delete);
				}

				if (deletesMap.containsKey(tblId)) {
					deletesMap.get(tblId).addAll(listDelete);
				} else {
					deletesMap.put(tblId, listDelete);
				}
			}
			log.debug("parse list data finished. used {} ms.", System.currentTimeMillis() - parseListStart);
		}

		log.info("parse kafka data finished. used {} ms.", System.currentTimeMillis() - start);
		doCommit(session, upsertsMap, deletesMap);

		return true;
	}

	/**
	 * 提交数据到Kudu中
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
				for (Upsert upsert : entry.getValue()) {
					session.apply(upsert);
				}
			}
			session.flush();
			log.error("commit upsert batch used {} ms.", System.currentTimeMillis() - start);

			start = System.currentTimeMillis();
			for (Entry<String, List<Delete>> entry : deletesMap.entrySet()) {
				for (Delete delete : entry.getValue()) {
					session.apply(delete);
				}
			}
			session.flush();
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

	public int getNumReplicas() {
		return numReplicas;
	}

	public void setNumReplicas(int numReplicas) {
		this.numReplicas = numReplicas;
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
