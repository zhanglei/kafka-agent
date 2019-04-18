package com.hncy58.kafka.producer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import com.alibaba.fastjson.JSONObject;

public class JsonDataProducerApp {

	private static int SEND_BATCH_SIZE = 5000;
	private static int SEND_BATCH_CNT = 60;
	private static int SEND_BATCH_INTERVAL = 1;

	public static final String[] ALPHA_ARR = new String[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b",
			"c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w",
			"x", "y", "z" };

	public static final String[] DB_ID_ARR = new String[] { "test" };
	// public static final String[] TBL_ID_ARR = new String[] { "ccs_customer",
	// "ccs_acct", "ccs_order"};
	public static final String[] TBL_ID_ARR = new String[] { "ccs_order_1", "ccs_order_2", "ccs_order_3", "ccs_order_4",
			"ccs_order_5", "ccs_order_6", "ccs_order_7", "ccs_order_8", "ccs_order_9", "ccs_order_10" };
	public static final String[] OPR_TYPE_ARR = new String[] { "i", "u", "d" };

	public static String TOPIC_NAME = "sit_sync_prodccsdb_0";
	public static String KAFKA_SERVERS = "test-9-238:9092";
	// public static String KAFKA_SERVERS = "192.168.144.128:9092";
	// public static String KAFKA_SERVERS =
	// "162.16.6.180:9092,162.16.6.181:9092,162.16.6.182:9092";

	public static boolean USE_TRANSACTION = false;

	private static Producer<String, String> producer;

	public static void main(String[] args) {

		System.out.println("Uage:\n\t" + JsonDataProducerApp.class.getName()
				+ " KAFKA_SERVERS TOPIC_NAME SEND_BATCH_SIZE SEND_BATCH_CNT SEND_BATCH_INTERVAL USE_TRANSACTION");
		System.out.println(
				"eg:\n\t" + JsonDataProducerApp.class.getName() + " localhost:9092 test_topic_1 10000 100 1 true");

		if (args.length > 0) {
			KAFKA_SERVERS = args[0].trim();
		}

		if (args.length > 1) {
			TOPIC_NAME = args[1].trim();
		}

		if (args.length > 2) {
			SEND_BATCH_SIZE = Integer.parseInt(args[2].trim());
		}

		if (args.length > 3) {
			SEND_BATCH_CNT = Integer.parseInt(args[3].trim());
		}

		if (args.length > 4) {
			SEND_BATCH_INTERVAL = Integer.parseInt(args[4].trim());
		}

		if (args.length > 5) {
			USE_TRANSACTION = Boolean.valueOf(args[5].trim());
		}

		if (USE_TRANSACTION) {
			producer = buildTransactionProducer();
			producer.initTransactions();
		} else {
			producer = buildUnTransactionProducer();
		}

		Random random = new Random();

		long cur_time = System.currentTimeMillis();

		while (SEND_BATCH_CNT > 0) {
			try {
				if (USE_TRANSACTION) {
					// 每一个最小循环进行一次事务提交
					producer.beginTransaction();
				}
				for (int k = 0; k < SEND_BATCH_SIZE; k++) {
					String key = Long.toString(cur_time - (SEND_BATCH_CNT * SEND_BATCH_SIZE + k));

					key = getHashPrefix(key, 12, 3) + key;

					// System.out.println(key);

					JSONObject json = new JSONObject(true);
					Map<String, Object> schema = new HashMap<>();
					List<Map<String, Object>> data = new ArrayList<>();
					schema.put("offset", key);
					schema.put("time", System.currentTimeMillis());
					schema.put("agt_svr_nm", "canal_01");
					// schema.put("db_id", DB_ID_ARR[random.nextInt(DB_ID_ARR.length)]);
					// schema.put("tbl_id", "ccs_order_1");
					// schema.put("opr_type", "i");
					schema.put("db_id", "");
					schema.put("tbl_id", TBL_ID_ARR[random.nextInt(TBL_ID_ARR.length)]);
					schema.put("opr_type", OPR_TYPE_ARR[random.nextInt(OPR_TYPE_ARR.length)]);
					schema.put("pk_col", "ORDER_ID");

					Map<String, Object> valueMap = new HashMap<>();
					valueMap.put("ORDER_ID", key);
					valueMap.put("ORDER_TIME", new Date());
					valueMap.put("ORBER_FAIL_TIME", new Date());
					valueMap.put("OPT_DATETIME", new Date());
					valueMap.put("CREATE_TIME", new Date());
					valueMap.put("LST_UPD_TIME", new Date());
					valueMap.put("ORG", random.nextInt(100000));
					valueMap.put("name", "name" + random.nextInt(100000));
					valueMap.put("cert_id", "43052119890625" + random.nextInt(10000));
					valueMap.put("gender", random.nextInt(2));
					valueMap.put("year_income", random.nextFloat() * 200000);
					valueMap.put("birth", System.currentTimeMillis());
					valueMap.put("remark",
							"2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0"
									+ "2019-04-18 10:43:24,893 INFO  com.hncy58.kafka.consumer.ConsumerToHBaseApp.doRun(ConsumerToHBaseApp.java:184) - no data to poll, sleep 5 s. buff size:0");
					data.add(valueMap);

					json.put("schema", schema);
					json.put("data", data);
					String value = json.toJSONString();
					Future<RecordMetadata> future = producer
							.send(new ProducerRecord<String, String>(TOPIC_NAME, key, value));
					// try {
					// System.out.println(future.get().offset() + "," +
					// future.get().partition());
					// } catch (InterruptedException e) {
					// e.printStackTrace();
					// } catch (ExecutionException e) {
					// e.printStackTrace();
					// }
				}
				if (USE_TRANSACTION) {
					producer.commitTransaction();
					producer.flush();
					System.out.println("one transaction batch datas commit finished. batch size:" + SEND_BATCH_SIZE);
				} else {
					producer.flush();
					System.out.println("one no transaction batch datas commit finished. batch size:" + SEND_BATCH_SIZE);
				}

				Thread.sleep(SEND_BATCH_INTERVAL * 1000);
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
				SEND_BATCH_CNT -= 1;
			}

		}

		// 将缓存的信息提交
		producer.flush();
		producer.close(10, TimeUnit.SECONDS);
	}

	private static String getHashPrefix(String key, int partitions, int length) {
		String tmp = "" + key.hashCode() % (partitions + 1);
		tmp = StringUtils.repeat("0", length - tmp.length()) + tmp;
		return tmp;
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
