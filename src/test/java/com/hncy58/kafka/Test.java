package com.hncy58.kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.kudu.Common.DataType;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

public class Test {

	public static void main(String[] args) {

		Arrays.asList("a,b c,c d, d , e, ".split(" *, *")).forEach(s -> System.out.println("---" + s + "---"));

		StringBuffer buf = new StringBuffer("_1234");
		System.out.println(buf.delete(0, 1).length());

		Arrays.asList(Type.values()).forEach(t -> {
			System.out.println(t);
			System.out.println(t.getSize());
			System.out.println(t.getName());
			System.out.println(t.getDataType());
		});
		System.out.println("----------------------");
		System.out.println(Type.valueOf("BOOL"));
		System.out.println(DataType.valueOf("UNIXTIME_MICROS"));

		System.out.println("-----------------");
		
		System.out.println(new Date(1528795344294L));

		System.out.println(castToLong("2018-06-12 17:22:24.294000"));

		System.out.println(new Date(castToLong("2018-06-12 17:22:24.294000")));
		
//		System.out.println(TypeUtils.castToLong("2018-06-12 17:22:24.294000"));

		String data = "{\"CUST_ID\":\"2018061211840535\",\"ORG_ID\":\"\",\"INTRODUCER_CODE\":\"\",\"CERT_TYPE\":\"0\",\"CORP_NAME\":\"\",\"CORP_ID\":\"\",\"CERT_EXPIRE_DATE\":\"\""
				+ ",\"APPLY_TYPE\":null,\"WORK_POSITION\":\"\",\"INTRODUCER_RELATION\":\"\",\"MOBILE_NO\":\"15315462220\",\"CREATE_DATE\":\"2018-06-12 17:22:24.294000\",\"INTRODUCER\":\"\",\"OPERATOR_ID\":\"\",\"MODIFY_DATE\":\"2018-06-12 17:22:24.294000\",\"ACCOUNT_NO\":\"\",\"CERT_ID\":\"432323199012075584\",\"REMARK\":\"\",\"CUST_NAME\":\"平昌\"}";

		JSONObject dataJson = JSONObject.parseObject(data);
		
		
		System.out.println(dataJson.toJSONString());
		System.out.println(dataJson.toString());
		

		for (Entry<String, Object> entry : dataJson.entrySet()) {
//			System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToBytes(entry.getValue()));
//			System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToBoolean(entry.getValue()));
//			System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToDouble(entry.getValue()));
//			System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToFloat(entry.getValue()));
//			System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToByte(entry.getValue()));
//			System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToInt(entry.getValue()));
//			System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToLong(entry.getValue()));
			if(entry.getKey().toLowerCase().equals("apply_type")) {
				System.out.println(entry.getValue());
				System.out.printf("%s %s", entry.getKey().toLowerCase(), TypeUtils.castToString(entry.getValue()));
				System.out.println("----------");
				System.out.printf("%s %s", entry.getKey().toLowerCase(), (long)TypeUtils.castToLong(entry.getValue()));
			}
		}
	}

	private static Long castToLong(Object date) {
		if (date == null || "".equals(date.toString().trim())) {
			return null;
		}
		String dateStr = date.toString().trim();
		if (dateStr.length() > 23) {
			dateStr = dateStr.substring(0, 23);
		}
		return TypeUtils.castToLong(dateStr);
	}
}
