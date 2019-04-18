package com.hncy58.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class ConsumerApp3 {

	// static int FETCH_SIZE = 128;
	static int FETCH_SIZE = 1;
	static int SLEEP_SECONDS = 5;

	private static Long total_msg = 0L;

	private static boolean PRINT_RECEIVED_DATA = false;

	public static void main(String[] args) {

		Properties props = new Properties();
		if (args.length > 0) {
			props.put("bootstrap.servers", args[0]);
		} else {
			props.put("bootstrap.servers", "192.168.144.128:9092");
		}

		if (args.length > 1) {
			FETCH_SIZE = Integer.parseInt(args[1].trim());
		}

		if (args.length > 2) {
			PRINT_RECEIVED_DATA = Boolean.valueOf(args[2].trim());
		}

		if (args.length > 3) {
			SLEEP_SECONDS = Integer.parseInt(args[3].trim());
		}

		props.put("group.id", "my-group-id");

		// props.put("enable.auto.commit", "true");
		// props.put("auto.commit.interval.ms", "1000");
		props.put("enable.auto.commit", "false");

		props.put("isolation.level", "read_committed");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

//		 consumer.subscribe(Arrays.asList(ProducerApp.TOPIC_NAME));

		final int minBatchSize = FETCH_SIZE;
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

		System.out.println(consumer.partitionsFor(ProducerApp.TOPIC_NAME));
		Set<TopicPartition> tps = new HashSet<>();
		consumer.partitionsFor(ProducerApp.TOPIC_NAME)
				.forEach(pi -> tps.add(new TopicPartition(pi.topic(), pi.partition())));

		consumer.assign(tps);
		
		consumer.beginningOffsets(tps).forEach((tp, offset) -> {
			System.out.println(tp);
			System.out.println(offset);
		});
		
		consumer.endOffsets(tps).forEach((tp, offset) -> {
			System.out.println(tp);
			System.out.println(offset);
		});


		tps.forEach(tp -> {
			System.out.println(consumer.committed(tp));
//			System.out.println(consumer.position(tp));
		});

		try {
			consumer.seek(new TopicPartition(ProducerApp.TOPIC_NAME, 0), 4020458);
			
			ConsumerRecords<String, String> records = consumer.poll(100);
			records.forEach(r -> {
				System.out.println(r.toString());
				Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
				offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
				consumer.commitSync(offsets);
			});
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	private static void handle(List<ConsumerRecord<String, String>> buffer) {
		System.out.println("start to handle datas -> " + buffer.size());
		if (PRINT_RECEIVED_DATA) {
			buffer.forEach(record -> {
				System.out.printf("key:%s, offset:%s, partition:%s, ts:%s, value:%s\n", record.key(), record.offset(),
						record.partition(), record.timestamp(), record.value());
			});
		}
		System.out.println("end handled datas.");
	}
}
