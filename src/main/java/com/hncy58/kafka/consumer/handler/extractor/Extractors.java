package com.hncy58.kafka.consumer.handler.extractor;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author	tokings
 * @company	hncy58	湖南长银五八
 * @website	http://www.hncy58.com
 * @version 1.0
 * @date	2019年4月22日 下午6:43:39
 *
 */
public class Extractors {
	
	private static final Map<String, Extractor> extractors = new HashMap<String, Extractor>();
	
	static {
		// 风控表客户进件GEO坐标特殊抽取器
		extractors.put("inf_address", new RiskInfAddrExtractor());
	}
	
	public static final Extractor get(String tableName) {
		return extractors.get(tableName);
	}
	
	public static boolean contains(String tableName) {
		return extractors.containsKey(tableName);
	}
}
