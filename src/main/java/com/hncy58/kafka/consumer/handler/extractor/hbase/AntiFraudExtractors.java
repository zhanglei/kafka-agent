package com.hncy58.kafka.consumer.handler.extractor.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hncy58.kafka.consumer.handler.extractor.Extractor;

public class AntiFraudExtractors {
	

	private static final Logger log = LoggerFactory.getLogger(AntiFraudExtractors.class);

	private static final Map<String, Extractor> extractors = new HashMap<String, Extractor>();

	static {
		// TODO init extractors. not implements yet
		
	}

	public static List<JSONObject> extract(JSONObject... srcDatas) {

		List<JSONObject> retDatas = new ArrayList<JSONObject>();

		for (JSONObject srcData : srcDatas) {

			if (srcData == null || srcData.isEmpty() || !srcData.containsKey("schema")
					|| !srcData.containsKey("data")) {
				log.warn("json value is null or empty, not contain schema,not contain data, ignored, record:{}",
						srcData);
				continue;
			}

			JSONObject schema = srcData.getJSONObject("schema");
			JSONArray jsonData = srcData.getJSONArray("data");
			if (jsonData.isEmpty()) {
				log.warn("json value data field is null, ignored, record:{}", srcData);
				continue;
			}

			String agt_svr_nm = schema.getString("agt_svr_nm");
			String dbId = schema.getString("db_id");
			String tblId = schema.getString("tbl_id");
			String oprType = schema.getString("opr_type");
			String pkCols = schema.getString("pk_col");
			String mapKey = (dbId == null || "".equals(dbId.trim())) ? tblId : dbId + ":" + tblId;
			
			
			
		}

		return retDatas;

	}

}
