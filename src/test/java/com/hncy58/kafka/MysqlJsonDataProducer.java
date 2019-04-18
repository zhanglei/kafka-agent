package com.hncy58.kafka;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.hncy58.ds.DataSourceUtil;

public class MysqlJsonDataProducer {

	public static boolean USE_TRANSACTION = true;

	private static String driverClass = "com.mysql.jdbc.Driver";
	private static DataSourceUtil dsUtil;

	private static Producer<String, String> producer;
	public static String TOPIC_NAME = "sit_sync_prodccsdb_0";
	public static String KAFKA_SERVERS = "162.16.6.180:9092,162.16.6.181:9092,162.16.6.182:9092";

	public static void main(String[] args) throws SQLException {

		int batchSize = 100;
		int batchTimes = 100;

		Map<String, String> sqls = new HashMap<>();
		sqls.put("ccs_acct", "select * from test.ccs_acct t limit " + batchSize + " offset 0");
		sqls.put("ccs_customer", "select * from test.ccs_customer t limit " + batchSize + " offset 0");
		sqls.put("ccs_loan", "select * from test.ccs_loan t limit " + batchSize + " offset 0");
		sqls.put("ccs_loan_reg", "select * from test.ccs_loan_reg t limit " + batchSize + " offset 0");
		sqls.put("ccs_loan_reg_hst", "select * from test.ccs_loan_reg_hst t limit " + batchSize + " offset 0");
		sqls.put("ccs_order", "select * from test.ccs_order t limit " + batchSize + " offset 0");
		sqls.put("ccs_order_hst", "select * from test.ccs_order_hst t limit " + batchSize + " offset 0");
		sqls.put("ccs_plan", "select * from test.ccs_plan t limit " + batchSize + " offset 0");
		sqls.put("ccs_repay_hst", "select * from test.ccs_repay_hst t limit " + batchSize + " offset 0");
		sqls.put("ccs_repay_schedule", "select * from test.ccs_repay_schedule t limit " + batchSize + " offset 0");
		sqls.put("customer_all_attrs", "select * from test.customer_all_attrs t limit " + batchSize + " offset 0");
		sqls.put("inf_customer", "select * from test.inf_customer t limit " + batchSize + " offset 0");
		sqls.put("inf_customer_credit", "select * from test.inf_customer_credit t limit " + batchSize + " offset 0");

		for (int i = 0; i < batchTimes; i++) {
			sqls.keySet().forEach(tableName -> {
				try {
					send(sqls.get(tableName), tableName);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			});
		}
	}

	private static void send(String sql, String tableName) throws SQLException {
		String url = "jdbc:mysql://162.16.9.238:4000/?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&zeroDateTimeBehavior=convertToNull";
		String username = "root";
		String password = "";
		int initialSize = 5;
		int maxTotal = 10;
		int maxIdle = 5;
		int maxWaitMillis = 5 * 1000;

		dsUtil = new DataSourceUtil();
		dsUtil.initDataSource(url, username, password, driverClass, initialSize, maxTotal, maxIdle, maxWaitMillis);

		if (USE_TRANSACTION) {
			producer = buildTransactionProducer();
			producer.initTransactions();
		} else {
			producer = buildUnTransactionProducer();
		}

		Connection conn = dsUtil.getConnection();

		try {

			Random random = new Random();
			ResultSet mdrs = conn.getMetaData().getPrimaryKeys("test", "%", tableName);
			String pkCols = "";
			while (mdrs.next()) {
				pkCols += "," + mdrs.getString("COLUMN_NAME");
			}

			if (USE_TRANSACTION) {
				producer.beginTransaction();
			}

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			ResultSetMetaData md = rs.getMetaData();

			while (rs.next()) {
				String key = Integer.toString(random.nextInt(10000000));
				JSONObject json = new JSONObject(true);
				Map<String, Object> schema = new HashMap<>();
				List<Map<String, Object>> data = new ArrayList<>();
				schema.put("offset", key);
				schema.put("time", System.currentTimeMillis());
				schema.put("agt_svr_nm", "canal_01");
				schema.put("db_id", "tidb_test");
				schema.put("tbl_id", tableName);
				schema.put("opr_type", "i");
				schema.put("pk_col", pkCols);

				Map<String, Object> valueMap = new HashMap<>();
				for (int i = 0; i < md.getColumnCount(); i++) {
					valueMap.put(md.getColumnName(i + 1), rs.getObject(i + 1));
				}
				data.add(valueMap);

				json.put("schema", schema);
				json.put("data", data);
				JSONObject.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; // fastjson默认日期格式
				String value = JSONObject.toJSONString(json, SerializerFeature.WriteDateUseDateFormat);

				// System.out.println(value);

				Future<RecordMetadata> future = producer
						.send(new ProducerRecord<String, String>(TOPIC_NAME, key, value));
			}

			if (USE_TRANSACTION) {
				producer.commitTransaction();
				producer.flush();
				System.out.println("one transaction batch datas commit finished.");
			} else {
				producer.flush();
				System.out.println("one no transaction batch datas commit finished.");
			}

			Thread.sleep(100);

		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			if (USE_TRANSACTION) {
				producer.abortTransaction();
			}
			producer.close();
		} catch (KafkaException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

		// 将缓存的信息提交
		producer.flush();
		producer.close(10, TimeUnit.SECONDS);
	}

	private static Producer<String, String> buildUnTransactionProducer() {
		Properties props = new Properties();
		// bootstrap.servers是Kafka集群的IP地址。多个时,使用逗号隔开
		props.put("bootstrap.servers", KAFKA_SERVERS);
		// 设置幂等性
		props.put("enable.idempotence", true);
		// Set acknowledgements for producer requests.
		props.put("acks", "all");
		props.put("retries", 1);

		// Specify buffer size in
		// config,这里不进行设置这个属性,如果设置了,还需要执行producer.flush()来把缓存中消息发送出去
		props.put("batch.size", 16384);
		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);
		// The buffer.memory controls the total amount of memory available to
		// the producer for buffering.
		props.put("buffer.memory", 33554432);
		// Kafka消息是以键值对的形式发送,需要设置key和value类型序列化器
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		return producer;
	}

	private static Producer<String, String> buildTransactionProducer() {
		// create instance for properties to access producer configs
		Properties props = new Properties();
		// bootstrap.servers是Kafka集群的IP地址。多个时,使用逗号隔开
		props.put("bootstrap.servers", KAFKA_SERVERS);
		// 设置事务id
		props.put("transactional.id", "first-transactional");
		// 设置幂等性
		props.put("enable.idempotence", true);
		// Set acknowledgements for producer requests.
		props.put("acks", "all");
		// If the request fails, the producer can automatically retry,
		props.put("retries", 1);

		// Specify buffer size in
		// config,这里不进行设置这个属性,如果设置了,还需要执行producer.flush()来把缓存中消息发送出去
		props.put("batch.size", 16384);
		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);
		// The buffer.memory controls the total amount of memory available to
		// the producer for buffering.
		props.put("buffer.memory", 33554432);
		// Kafka消息是以键值对的形式发送,需要设置key和value类型序列化器
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		return producer;
	}
}
