package com.hncy58.kafka;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

public class FastJsonTest {

	public static void main(String[] args) {
		String json = "{\"data\":[{\"id\":\"1\",\"name\":\"Json技术\"},{\"id\":\"2\",\"name\":\"java技术\"}]}";
		//将json转换成List
		Map list = JSON.parseObject(json,new TypeReference<Map>(){});
		
		System.out.println(list);
		
		list.forEach((k, v) -> {
			System.out.println(v.getClass().getName());
			System.out.println(k);
			System.out.println(v);
		});
	}
}
