package com.hncy58.kafka.consumer.handler;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Handler {

	public boolean handle(List<ConsumerRecord<String, String>> buffer) throws Exception;
	
	public void onHandleFail(List<ConsumerRecord<String, String>> buffer) throws Exception;
}
