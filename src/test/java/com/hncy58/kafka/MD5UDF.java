package com.hncy58.kafka;

import java.security.MessageDigest;

import org.apache.hadoop.hive.ql.exec.UDF;

@SuppressWarnings("deprecation")
public class MD5UDF extends UDF {

	public String evaluate(final String... strs) {

		if (strs == null) {
			return null;
		}
		String str = "";
		for (int i = 0; i < strs.length; i++) {
			if (strs[i] != null) {
				str += strs[i];
			}
		}

		if ("".equals(str)) {
			return "";
		}

		String digest = null;
		StringBuffer buffer = new StringBuffer();
		try {
			MessageDigest digester = MessageDigest.getInstance("md5");
			byte[] digestArray = digester.digest(str.getBytes("UTF-8"));
			for (int i = 0; i < digestArray.length; i++) {
				buffer.append(String.format("%02x", digestArray[i]));
			}
			digest = buffer.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return digest;
		// return digest != null ? digest.toUpperCase() : digest;
	}

	public static void main(String[] args) {
		MD5UDF md5 = new MD5UDF();
		System.out.println(md5.evaluate("tdz", "test"));
		System.out.println(md5.evaluate(null));
	}
}