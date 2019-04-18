package com.hncy58.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

public class TransactionApp {
	public static void main(String[] args) {

		// Producer 配置信息，应该配置在属性文件中
		Properties props = new Properties();
		//指定要连接的 broker，不需要列出所有的 broker，但建议至少列出2个，以防某个 broker 挂了

		if(args.length > 0) {
			props.put("bootstrap.servers", args[0]);
		} else {
			props.put("bootstrap.servers", "192.168.144.128:9092");
		}
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("transactional.id", "test.transactional.id"); // 需要设置 transactional.id

		// 创建 Producer
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		// 初始化事务
		producer.initTransactions();
		
		try {
			// 开启事务
			producer.beginTransaction();
			
			// 发送消息
			producer.send(new ProducerRecord<String, String>("test-topic-1", "message 5"), new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception != null) {
						System.out.println("send message5 failed with " + exception.getMessage());
					} else {
						// offset 是消息在 partition 中的编号，可以根据 offset 检索消息
						System.out.println("message5 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());	
					}
					
				}
				
			});
			
			// 提交事务
			producer.commitTransaction();
			
		} catch(KafkaException e) {
			// 终止事务
			producer.abortTransaction();
		} finally {
			producer.close();
		}

	}
}
