package com.hncy58.kafka.consumer.handler.extractor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.hncy58.util.Ellipsoid;
import com.hncy58.util.GEODistanceCaculator;

/**
 * 风控客户地址信息表特殊字段抽取
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2019年4月22日 下午3:44:56
 *
 */
public class RiskInfAddrExtractor implements Extractor<JSONObject> {
	
	private static final Logger LOG = LoggerFactory.getLogger(RiskInfAddrExtractor.class);

	private static final Set<String> EXTRACT_COLS = new HashSet<String>(Arrays.asList("LBS_ADDRESS", "lbs_latitude"));
	private static final String ADDR_SPLIT_PATTERN = " *- *";
	private static final double EARTH_RADIUS = Ellipsoid.Sphere.getSemiMajorAxis();

	@Override
	public JSONObject extract(JSONObject src) throws Exception {

		long start = System.currentTimeMillis();
		
		EXTRACT_COLS.forEach(col -> {
			switch (col) {
			case "LBS_ADDRESS": // 抽取省、市、县
				if (null == src.get(col) || "".equals(src.getString(col).trim()))
					break;
				String[] addrSegs = src.getString(col).split(ADDR_SPLIT_PATTERN);
				if (addrSegs.length > 1) {
					src.put("LBS_PROVINCE", addrSegs[0]); // 省
				}
				if (addrSegs.length > 2) {
					src.put("LBS_CITY", addrSegs[1]); // 市
				}
				if (addrSegs.length > 3) {
					src.put("LBS_COUNTY", addrSegs[2]); // 县/区
				}

				break;
			case "lbs_latitude": // 计算进件地址
				if (null == src.get("lbs_latitude") || null == src.get("lbs_longitude")
						|| null == src.get("salesman_latitude") || null == src.get("salesman_longitude")
						|| "".equals(src.getString("lbs_latitude").trim())
						|| "".equals(src.getString("lbs_longitude").trim())
						|| "".equals(src.getString("salesman_latitude").trim())
						|| "".equals(src.getString("salesman_longitude").trim()))
					break;

				double lbs_latitude = src.getDoubleValue("lbs_latitude");
				double lbs_longitude = src.getDoubleValue("lbs_longitude");
				double salesman_latitude = src.getDoubleValue("salesman_latitude");
				double salesman_longitude = src.getDoubleValue("salesman_longitude");

				double cust_introducer_distance = GEODistanceCaculator.distanceOfTwoGEOPoints(EARTH_RADIUS,
						lbs_latitude, lbs_longitude, salesman_latitude, salesman_longitude);

				src.put("cust_introducer_distance", cust_introducer_distance);

				break;
			default:
				break;
			}
		});

		LOG.info("extract risk inf addr used {} ms.", System.currentTimeMillis() - start);
		
		return src;
	}

	public static void main(String[] args) throws Exception {

		RiskInfAddrExtractor extractor = new RiskInfAddrExtractor();

		JSONObject src = new JSONObject();
		src.put("LBS_ADDRESS", "湖南省-长沙市-开福区-芙蓉中路一段433号");
		src.put("lbs_latitude", "112.986091");
		src.put("lbs_longitude", "28.207006");
		src.put("salesman_latitude", "112.986091");
		src.put("salesman_longitude", "28.207006");

		System.out.println(src.getDoubleValue("lbs_latitude"));

		System.out.println(extractor.extract(src));

	}

}
