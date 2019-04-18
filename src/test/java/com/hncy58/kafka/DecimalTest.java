package com.hncy58.kafka;

import com.alibaba.fastjson.util.TypeUtils;

public class DecimalTest {

	public static void main(String[] args) {
		System.out.println(TypeUtils.castToBigDecimal("123.45670"));
		System.out.println(TypeUtils.castToBigDecimal(123.45670));
		System.out.println(TypeUtils.castToBigDecimal("123.45670"));
	}
}
