package com.hncy58.kafka.consumer.handler;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.sql.SQLException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;
import com.hncy58.ds.ServerStatusReportUtil;
import com.hncy58.util.TiDBUtil;
import com.hncy58.util.Utils;

/**
 * 写入数据至TiDB表
 * 
 * @author tdz
 * @company hncy58 长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2019年2月25日 下午4:42:26
 */
public class TiDBHandler implements Handler {

	private static final Logger log = LoggerFactory.getLogger(TiDBHandler.class);
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat dateSdf = new SimpleDateFormat("yyyy-MM-dd");

	private String agentSvrName;
	private String agentSvrGroup;
	private int agentSvrType;

	private String localFileNamePrefix = "unHadledData";

	private String tidbURL;
	private String tidbUserName;
	private String tidbPasswd;
	private int tidbInitialSize;
	private int tidbMaxTotal;
	private int tidbMaxIdle;
	private int tidbMaxWaitMillis;

	private int tidbBatchSize;

	public TiDBHandler(String agentSvrName, String agentSvrGroup, int agentSvrType, String localFileNamePrefix,
			String tidbURL, String tidbUserName, String tidbPasswd, int tidbInitialSize, int tidbMaxTotal,
			int tidbMaxIdle, int tidbMaxWaitMillis, int tidbBatchSize) throws Exception {
		super();
		this.agentSvrName = agentSvrName;
		this.agentSvrGroup = agentSvrGroup;
		this.agentSvrType = agentSvrType;
		this.localFileNamePrefix = localFileNamePrefix;
		this.tidbURL = tidbURL;
		this.tidbUserName = tidbUserName;
		this.tidbPasswd = tidbPasswd;
		this.tidbInitialSize = tidbInitialSize;
		this.tidbMaxTotal = tidbMaxTotal;
		this.tidbMaxIdle = tidbMaxIdle;
		this.tidbMaxWaitMillis = tidbMaxWaitMillis;
		this.tidbBatchSize = tidbBatchSize;

		init();
	}

	/**
	 * 初始化
	 * 
	 * @throws Exception
	 */
	private void init() throws Exception {

		if (TiDBUtil.isInited()) {

		} else {
			boolean inited = TiDBUtil.initDB(tidbURL, tidbUserName, tidbPasswd, tidbInitialSize, tidbMaxTotal,
					tidbMaxIdle, tidbMaxWaitMillis);
			while (!inited) {
				log.error("inited tidb faied, ret -> {}, start to redo init opt.");
				inited = TiDBUtil.initDB(tidbURL, tidbUserName, tidbPasswd, tidbInitialSize, tidbMaxTotal, tidbMaxIdle,
						tidbMaxWaitMillis);
			}
		}
	}

	@Override
	public boolean handle(List<ConsumerRecord<String, String>> data) throws Exception {

		if (data == null || data.isEmpty())
			return true;

		Map<String, String> tableSchema;

		List<String> insertSqls = new ArrayList<>();
		List<String> updateSqls = new ArrayList<>();
		List<String> deleteSqls = new ArrayList<>();

		String tblId;
		String dbId;
		long syncTime;
		JSONObject schemaJson;
		JSONArray jsonDataArr;
		JSONObject json = null;
		String value;
		String oprType;
		String pkCols;
		ArrayList<String> rowKeys = new ArrayList<>();
		Set<String> unExistTable = new HashSet<>();

		log.info("start parse kafka data.");
		long start = System.currentTimeMillis();
		long tmpCnt = 0L;

		for (ConsumerRecord<String, String> r : data) {
			value = r.value();
			// for debug
			log.debug("received data -> {}", value);

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

			schemaJson = json.getJSONObject("schema");
			jsonDataArr = json.getJSONArray("data");
			if (jsonDataArr.isEmpty()) {
				log.error("json value data field is null, ignored, record:{}", r);
				continue;
			}

			tblId = schemaJson.getString("tbl_id");
			dbId = schemaJson.getString("db_id");
			syncTime = schemaJson.getLong("time");
			tblId = Utils.isEmpty(dbId) ? tblId : dbId + "." + tblId;

			if (!TiDBUtil.getTableSchemas().containsKey(tblId)) {
				unExistTable.add(tblId);
				// log.debug("Kudu表:{}对应Schema不存在或者未加载成功，数据被忽略 ,data:\n{}",
				// tblId, value);
				log.warn("Kudu表:{}对应Schema不存在或者未加载成功，数据被忽略", tblId);
				continue;
			}

			tableSchema = TiDBUtil.getTableSchemas().get(tblId);

			oprType = schemaJson.getString("opr_type");
			pkCols = schemaJson.getString("pk_col");

			if (pkCols != null && !"".equals(pkCols.trim())) {
				rowKeys.clear();
				rowKeys.addAll(Arrays.asList(pkCols.split(" *, *")));
			}

			log.debug("start parse list data.");
			long parseListStart = System.currentTimeMillis();
			for (Object dataObj : jsonDataArr) {
				JSONObject dataJson = null;
				if (dataObj instanceof JSONObject) {
					dataJson = (JSONObject) dataObj;
				} else {
					log.error("data child is not correct json :{}", dataObj);
					continue;
				}

				String sql = null;
				switch (oprType) {
				case "i":
					sql = fillInsertRow(tblId, tableSchema, rowKeys, dataJson);
					insertSqls.add(sql);
					break;
				case "u":
					sql = fillUpdateRow(tblId, tableSchema, rowKeys, dataJson);
					updateSqls.add(sql);
					break;
				case "d":
					sql = fillDeleteRow(tblId, tableSchema, rowKeys, dataJson);
					deleteSqls.add(sql);
					break;
				default:
					log.error("not correct oprType:{}", oprType);
					break;
				}

				log.debug("parsed sql -> {}", sql);
				++tmpCnt;
			}

			log.debug("parse list data finished, used {} ms.", System.currentTimeMillis() - parseListStart);
		}

		if (!unExistTable.isEmpty()) {
			ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
					"Kudu表不存在或者未加载成功，数据被忽略，tableList:" + unExistTable);
		}

		log.error("parse kafka data finished size:{}, used {} ms.", tmpCnt, System.currentTimeMillis() - start);

		doCommit(insertSqls, updateSqls, deleteSqls);

		return true;
	}

	private void doCommit(List<String> insertSqls, List<String> updateSqls, List<String> deleteSqls)
			throws SQLException {

		List<String> tmpSqls = new ArrayList<>();

		if (insertSqls != null && !insertSqls.isEmpty()) {
			for (String sql : insertSqls) {
				tmpSqls.add(sql);
				if (tmpSqls.size() >= tidbBatchSize) {
					TiDBUtil.batchInTransaction(tmpSqls);
					tmpSqls.clear();
				}
			}

			if (!tmpSqls.isEmpty()) {
				TiDBUtil.batchInTransaction(tmpSqls);
				tmpSqls.clear();
			}
		}

		if (updateSqls != null && !updateSqls.isEmpty()) {
			for (String sql : updateSqls) {
				tmpSqls.add(sql);
				if (tmpSqls.size() >= tidbBatchSize) {
					TiDBUtil.batchInTransaction(tmpSqls);
					tmpSqls.clear();
				}
			}

			if (!tmpSqls.isEmpty()) {
				TiDBUtil.batchInTransaction(tmpSqls);
				tmpSqls.clear();
			}
		}

		if (deleteSqls != null && !deleteSqls.isEmpty()) {
			for (String sql : deleteSqls) {
				tmpSqls.add(sql);
				if (tmpSqls.size() >= tidbBatchSize) {
					TiDBUtil.batchInTransaction(tmpSqls);
					tmpSqls.clear();
				}
			}

			if (!tmpSqls.isEmpty()) {
				TiDBUtil.batchInTransaction(tmpSqls);
				tmpSqls.clear();
			}
		}
	}

	private String fillInsertRow(String tblId, Map<String, String> tableSchema, ArrayList<String> rowKeys,
			JSONObject jsonRow) {

		StringBuffer sql = new StringBuffer("insert into ");
		sql.append(tblId);

		StringBuffer keys = new StringBuffer("(");
		StringBuffer values = new StringBuffer("(");

		jsonRow.keySet().forEach(key -> {
			if (!tableSchema.containsKey(key.toUpperCase())) {
				return;
			}
			// 判断是如果为空或者NULL等字符串则直接设置为null
			if(null == jsonRow.get(key) || "null".equalsIgnoreCase(jsonRow.get(key).toString().trim())) {
				keys.append(key).append(",");
				values.append("null").append(",");
				return;
			}

			switch (tableSchema.get(key.toUpperCase())) {
			case "VARCHAR":
				keys.append(key).append(",");
				values.append("'").append(jsonRow.getString((key)).replaceAll("'", "\\\\'")).append("'").append(",");
//				values.append("'").append(jsonRow.getString((key))).append("'").append(",");
				break;
			case "BOOLEAN":
				keys.append(key).append(",");
				values.append(jsonRow.getBoolean(key)).append(",");
				break;
			case "DOUBLE":
				keys.append(key).append(",");
				values.append(jsonRow.getDouble(key)).append(",");
				break;
			case "FLOAT":
				keys.append(key).append(",");
				values.append(jsonRow.getFloat(key)).append(",");
				break;
			case "INT":
				keys.append(key).append(",");
				values.append(jsonRow.getInteger(key)).append(",");
				break;
			case "BIGINT":
				keys.append(key).append(",");
				values.append(jsonRow.getInteger(key)).append(",");
				break;
			case "SMALLINT":
				keys.append(key).append(",");
				values.append(jsonRow.getInteger(key)).append(",");
				break;
			case "TINYINT":
				keys.append(key).append(",");
				values.append(jsonRow.getInteger(key)).append(",");
				break;
			case "DECIMAL":
				keys.append(key).append(",");
				values.append(castToDecimal(jsonRow.get(key))).append(",");
				break;
			case "TIMESTAMP":
				keys.append(key).append(",");
				values.append("'").append(sdf.format(jsonRow.getTimestamp(key))).append("'").append(",");
				break;
			case "DATE":
				keys.append(key).append(",");
				values.append("'").append(dateSdf.format(jsonRow.getDate(key))).append("'").append(",");
				break;
			default:
				keys.append(key).append(",");
				values.append("'").append(jsonRow.get(key).toString().replaceAll("'", "\\\\'")).append("'").append(",");
				break;
			}
		});

		if (keys.length() > 1) {
			sql.append(keys.substring(0, keys.lastIndexOf(","))).append(") values")
					.append(values.substring(0, values.lastIndexOf(","))).append(")");
		} else {
			return null;
		}

		return sql.toString();
	}

	private String fillUpdateRow(String tblId, Map<String, String> tableSchema, ArrayList<String> rowKeys,
			JSONObject jsonRow) {

		if (rowKeys == null || rowKeys.isEmpty()) {
			return null;
		}

		StringBuffer sql = new StringBuffer("update ");
		sql.append(tblId).append(" set ");
		StringBuffer setBuf = new StringBuffer();
		StringBuffer whereBuf = new StringBuffer();

		jsonRow.keySet().forEach(key -> {
			if (!tableSchema.containsKey(key.toUpperCase()) || rowKeys.contains(key)) {
				return;
			}
			// 判断是如果为空或者NULL等字符串则直接设置为null
			if(null == jsonRow.get(key) || "null".equalsIgnoreCase(jsonRow.get(key).toString().trim())) {
				setBuf.append(key).append("=").append("null").append(",");
				return;
			}

			switch (tableSchema.get(key.toUpperCase())) {
			case "VARCHAR":
				setBuf.append(key).append("=").append("'").append(jsonRow.getString((key)).replaceAll("'", "\\\\'")).append("'").append(",");
				break;
			case "BOOLEAN":
				setBuf.append(key).append("=").append(jsonRow.getBoolean((key))).append(",");
				break;
			case "DOUBLE":
				setBuf.append(key).append("=").append(jsonRow.getDouble((key))).append(",");
				break;
			case "FLOAT":
				setBuf.append(key).append("=").append(jsonRow.getFloat((key))).append(",");
				break;
			case "INT":
				setBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(",");
				break;
			case "BIGINT":
				setBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(",");
				break;
			case "SMALLINT":
				setBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(",");
				break;
			case "TINYINT":
				setBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(",");
				break;
			case "DECIMAL":
				setBuf.append(key).append("=").append(castToDecimal(jsonRow.get(key))).append(",");
				break;
			case "TIMESTAMP":
				setBuf.append(key).append("=").append("'").append(sdf.format(jsonRow.getTimestamp(key))).append("'").append(",");
				break;
			case "DATE":
				setBuf.append(key).append("=").append("'").append(dateSdf.format(jsonRow.getDate(key))).append("'").append(",");
				break;
			default:
				setBuf.append(key).append("=").append("'").append(jsonRow.get((key)).toString().replaceAll("'", "\\\\'")).append("'").append(",");
				break;
			}
		});

		if (setBuf.length() > 1) {
			sql.append(setBuf.substring(0, setBuf.lastIndexOf(",")));
		} else {
			return null;
		}

		whereBuf.append(" where ");
		rowKeys.forEach(key -> {
			if (!tableSchema.containsKey(key.toUpperCase())) {
				return;
			}

			// 判断是如果为空或者NULL等字符串则直接设置为null
			if(null == jsonRow.get(key) || "null".equalsIgnoreCase(jsonRow.get(key).toString().trim())) {
				whereBuf.append(key).append("=").append("null").append(" and ");
				return;
			}
			
			switch (tableSchema.get(key.toUpperCase())) {
			case "VARCHAR":
				whereBuf.append(key).append("=").append("'").append(jsonRow.getString((key)).replaceAll("'", "\\\\'")).append("'")
						.append(" and ");
				break;
			case "BOOLEAN":
				whereBuf.append(key).append("=").append(jsonRow.getBoolean((key))).append(" and ");
				break;
			case "DOUBLE":
				whereBuf.append(key).append("=").append(jsonRow.getDouble((key))).append(" and ");
				break;
			case "FLOAT":
				whereBuf.append(key).append("=").append(jsonRow.getFloat((key))).append(" and ");
				break;
			case "INT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "BIGINT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "SMALLINT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "TINYINT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "DECIMAL":
				whereBuf.append(key).append("=").append(castToDecimal(jsonRow.get(key))).append(" and ");
				break;
			case "TIMESTAMP":
				whereBuf.append(key).append("=").append("'").append(sdf.format(jsonRow.getTimestamp(key))).append("'")
						.append(" and ");
				break;
			case "DATE":
				whereBuf.append(key).append("=").append("'").append(dateSdf.format(jsonRow.getDate(key))).append("'")
				.append(" and ");
				break;
			default:
				whereBuf.append(key).append("=").append("'").append(jsonRow.get((key)).toString().replaceAll("'", "\\\\'")).append("'").append(" and ");
				break;
			}
		});

		if (whereBuf.length() > 7) {
			sql.append(whereBuf.substring(0, whereBuf.lastIndexOf(" and ")));
		} else {
			return null;
		}

		return sql.toString();
	}

	private String fillDeleteRow(String tblId, Map<String, String> tableSchema, ArrayList<String> rowKeys,
			JSONObject jsonRow) {

		if (rowKeys == null || rowKeys.isEmpty()) {
			return null;
		}

		StringBuffer sql = new StringBuffer("delete from ");
		sql.append(tblId);
		StringBuffer whereBuf = new StringBuffer(" where ");
		rowKeys.forEach(key -> {
			if (!tableSchema.containsKey(key.toUpperCase())) {
				return;
			}
			switch (tableSchema.get(key.toUpperCase())) {
			case "VARCHAR":
				whereBuf.append(key).append("=").append("'").append(jsonRow.getString((key)).replaceAll("'", "\\\\'")).append("'")
						.append(" and ");
				break;
			case "BOOLEAN":
				whereBuf.append(key).append("=").append(jsonRow.getBoolean((key))).append(" and ");
				break;
			case "DOUBLE":
				whereBuf.append(key).append("=").append(jsonRow.getDouble((key))).append(" and ");
				break;
			case "FLOAT":
				whereBuf.append(key).append("=").append(jsonRow.getFloat((key))).append(" and ");
				break;
			case "INT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "BIGINT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "SMALLINT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "TINYINT":
				whereBuf.append(key).append("=").append(jsonRow.getInteger((key))).append(" and ");
				break;
			case "DECIMAL":
				whereBuf.append(key).append("=").append(castToDecimal(jsonRow.get(key))).append(" and ");
				break;
			case "TIMESTAMP":
				whereBuf.append(key).append("=").append("'").append(sdf.format(jsonRow.getTimestamp(key))).append("'")
						.append(" and ");
				break;
			case "DATE":
				whereBuf.append(key).append("=").append("'").append(dateSdf.format(jsonRow.getDate(key))).append("'")
				.append(" and ");
				break;
			default:
				whereBuf.append(key).append("=").append("'").append(jsonRow.get((key)).toString().replaceAll("'", "\\\\'")).append("'").append(" and ");
				break;
			}
		});

		if (whereBuf.length() > 7) {
			sql.append(whereBuf.substring(0, whereBuf.lastIndexOf(" and ")));
		} else {
			return null;
		}

		return sql.toString();
	}

	private BigDecimal castToDecimal(Object data) {

		if (data instanceof Number)
			return TypeUtils.castToBigDecimal(data);

		if (data == null || "".equals(data.toString().trim())) {
			return null;
		}

		return new BigDecimal(data.toString().trim());
		// return TypeUtils.castToBigDecimal(data);
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
		return tidbURL;
	}

	public void setKuduMaster(String kuduMaster) {
		this.tidbURL = kuduMaster;
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

	public String getTidbURL() {
		return tidbURL;
	}

	public String getTidbUserName() {
		return tidbUserName;
	}

	public String getTidbPasswd() {
		return tidbPasswd;
	}

	public int getTidbInitialSize() {
		return tidbInitialSize;
	}

	public int getTidbMaxTotal() {
		return tidbMaxTotal;
	}

	public int getTidbMaxIdle() {
		return tidbMaxIdle;
	}

	public int getTidbMaxWaitMillis() {
		return tidbMaxWaitMillis;
	}

}
