package com.hncy58.kafka.consumer.handler;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class HBaseHandler implements Handler {

	private static final Logger log = LoggerFactory.getLogger(HBaseHandler.class);

	private String zkServers = "localhsot";
	private String zkPort = "2181";
	private String hbaseColumnFamilyName = "info";
	private String localFileNamePrefix = "unHadledData";

	public HBaseHandler(String zkServers, String zkPort, String hbaseColumnFamilyName, String localFileNamePrefix) {
		super();
		this.zkServers = zkServers;
		this.zkPort = zkPort;
		this.hbaseColumnFamilyName = hbaseColumnFamilyName;
		this.localFileNamePrefix = localFileNamePrefix;
	}

	@Override
	public boolean handle(List<ConsumerRecord<String, String>> data) throws Exception {

		if (data == null || data.isEmpty())
			return true;

		byte[] cfBytes = Bytes.toBytes(hbaseColumnFamilyName);
		Map<String, List<Put>> insertPutsMap = new HashMap<>();
		Map<String, List<Delete>> deletePutsMap = new HashMap<>();
		long start = System.currentTimeMillis();
		long allStart = System.currentTimeMillis();

		data.forEach(r -> {
			String value = r.value();
			if (StringUtils.isEmpty(value)) {
				log.warn("data value is null, ignored, record:{}", r);
				return;
			}

			JSONObject json = null;
			try {
				json = JSONObject.parseObject(value);
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}

			if (json == null || json.isEmpty() || !json.containsKey("schema") || !json.containsKey("data")) {
				log.warn("json value is null or empty, not contain schema,not contain data, ignored, record:{}", r);
				return;
			}

			JSONObject schema = json.getJSONObject("schema");
			JSONArray jsonData = json.getJSONArray("data");
			if (jsonData.isEmpty()) {
				log.warn("json value data field is null, ignored, record:{}", r);
				return;
			}

			String agt_svr_nm = schema.getString("agt_svr_nm");
			String dbId = schema.getString("db_id");
			String tblId = schema.getString("tbl_id");
			String oprType = schema.getString("opr_type");
			String pkCols = schema.getString("pk_col");
			String mapKey = (dbId == null || "".equals(dbId.trim())) ? tblId : dbId + ":" + tblId;

			final ArrayList<String> rowKeys = new ArrayList<>(Arrays.asList(r.key()));
			if (pkCols != null && !"".equals(pkCols.trim())) {
				rowKeys.clear();
				rowKeys.addAll(Arrays.asList(pkCols.split(" *, *")));
				Collections.sort(rowKeys);
			}

			if ("i".equals(oprType) || "u".equals(oprType)) {
				List<Put> listPut = new ArrayList<Put>();
				jsonData.forEach(dataObj -> {
					JSONObject dataJson = null;
					if (dataObj instanceof JSONObject) {
						dataJson = (JSONObject) dataObj;
					} else {
						log.warn("data child is not correct json :{}", dataObj);
						return;
					}

					StringBuffer idBuf = new StringBuffer("");
					for (String id : rowKeys) {
						idBuf.append("_" + dataJson.getString(id));
					}

					if (idBuf.length() > 0) {
						Put put = new Put(Bytes.toBytes(idBuf.delete(0, 1).toString()), r.timestamp());
						put.addColumn(cfBytes, Bytes.toBytes("ts"), Bytes.toBytes(schema.getString("time")));
						put.addColumn(cfBytes, Bytes.toBytes("offset"), Bytes.toBytes(schema.getString("offset")));
						put.addColumn(cfBytes, Bytes.toBytes("syncOprType"), Bytes.toBytes(oprType));
						// put.addColumn(cfBytes, Bytes.toBytes("time"), Bytes.toBytes(schema.getString("time")));

						dataJson.entrySet().forEach(entry -> {
							put.addColumn(cfBytes, Bytes.toBytes(entry.getKey()),
									Bytes.toBytes(entry.getValue().toString()));
						});

						listPut.add(put);
					}
				});

				if (insertPutsMap.containsKey(mapKey)) {
					insertPutsMap.get(mapKey).addAll(listPut);
				} else {
					insertPutsMap.put(mapKey, listPut);
				}
			} else if ("d".equals(oprType)) {
				List<Delete> listDelete = new ArrayList<Delete>();
				jsonData.forEach(dataObj -> {
					JSONObject dataJson = null;
					if (dataObj instanceof JSONObject) {
						dataJson = (JSONObject) dataObj;
					} else {
						log.warn("data child is not correct json :{}", dataObj);
						return;
					}

					StringBuffer idBuf = new StringBuffer("");
					for (String id : rowKeys) {
						idBuf.append("_" + dataJson.getString(id));
					}

					if (idBuf.length() > 0) {
						Delete delete = new Delete(Bytes.toBytes(idBuf.delete(0, 1).toString()), r.timestamp());
						listDelete.add(delete);
					}
				});

				if (deletePutsMap.containsKey(mapKey)) {
					deletePutsMap.get(mapKey).addAll(listDelete);
				} else {
					deletePutsMap.put(mapKey, listDelete);
				}
			}
		});

		log.error("parse all data size {}, used {} ms.", insertPutsMap.size() + deletePutsMap.size(),
				System.currentTimeMillis() - start);

		if (insertPutsMap.isEmpty() && deletePutsMap.isEmpty())
			return true;

		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", getZkServers());
		hbaseConf.set("hbase.zookeeper.property.clientPort", getZkPort());
		hbaseConf.set("hbase.defaults.for.version.skip", "true");
		start = System.currentTimeMillis();
		Connection hbaseConn = null;
		try {
			hbaseConn = ConnectionFactory.createConnection(hbaseConf);
			log.error("connect hbase, used {} ms.", System.currentTimeMillis() - start);
			for (Entry<String, List<Put>> entry : insertPutsMap.entrySet()) {
				start = System.currentTimeMillis();
				Table table = null;
				try {
					table = hbaseConn.getTable(TableName.valueOf(entry.getKey()));
					table.put(entry.getValue());
				} finally {
					if (table != null) {
						table.close();
					}
				}
				log.error("commit upsert size {}, used {} ms.", entry.getValue().size(),
						System.currentTimeMillis() - start);
			}

			for (Entry<String, List<Delete>> entry : deletePutsMap.entrySet()) {
				start = System.currentTimeMillis();
				Table table = null;
				int size = entry.getValue().size();
				try {
					table = hbaseConn.getTable(TableName.valueOf(entry.getKey()));
					table.delete(entry.getValue());
				} finally {
					if (table != null) {
						table.close();
					}
				}
				log.error("commit delete size {}, used {} ms.", size, System.currentTimeMillis() - start);
			}
		} finally {
			if (hbaseConn != null && !hbaseConn.isClosed()) {
				hbaseConn.close();
			}
		}

		log.error("all data finished, used {} ms.", System.currentTimeMillis() - allStart);

		return true;
	}

	@Override
	public void onHandleFail(List<ConsumerRecord<String, String>> data) throws Exception {

		if (data == null || data.isEmpty())
			return;

		long startOffset = data.get(0).offset();
		long endOffset = data.get(data.size() - 1).offset();
		Map<String, StringBuffer> buffMap = new HashMap<>();

		log.warn("start to store datas to local -> " + data.size());
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

		log.warn("end stored datas to local.");
	}

	public String getZkServers() {
		return zkServers;
	}

	public void setZkServers(String zkServers) {
		this.zkServers = zkServers;
	}

	public String getZkPort() {
		return zkPort;
	}

	public void setZkPort(String zkPort) {
		this.zkPort = zkPort;
	}

	public String getHbaseColumnFamilyName() {
		return hbaseColumnFamilyName;
	}

	public void setHbaseColumnFamilyName(String hbaseColumnFamilyName) {
		this.hbaseColumnFamilyName = hbaseColumnFamilyName;
	}

}
