package com.hncy58.kafka;
import com.hncy58.util.PropsUtil;

public class PropsTest {

	public static void main(String[] args) {
		
		System.out.println(PropsUtil.get("kafka-to-hdfs.agentSvrName"));
		System.out.println(PropsUtil.get("kafka-to-hdfs", "agentSvrName"));
		System.out.println(PropsUtil.getWithDefault("kafka-to-hdfs.agentSvrName", "abc"));
		System.out.println(PropsUtil.getWithDefault("kafka-to-hdfs", "agentSvrName", "def"));
	}
}
