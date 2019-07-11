package com.hncy58.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.hncy58.kafka.consumer.KafkaToKuduSpecialExtractApp;

/**
 * 短信发送工具类
 * 
 * @author tdz
 * @company hncy58 长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2019年7月5日 下午2:57:00
 */
public class SendMsgUtil {

	private static final Logger log = LoggerFactory.getLogger(SendMsgUtil.class);

	/**
	 * 发送短信请求的URL地址
	 */
	private static final String SEND_MSG_URL = PropsUtil.getWithDefault(KafkaToKuduSpecialExtractApp.PROP_PREFIX,
			"sendMsgUrl", "");

	/**
	 * 是否发送短信
	 */
	private static final boolean SEND_MSG = Boolean
			.valueOf(PropsUtil.getWithDefault(KafkaToKuduSpecialExtractApp.PROP_PREFIX, "sendMsg", "false"));

	/**
	 * 待发送短信号码列表
	 */
	private static final List<String> SEND_PHONES = Arrays
			.asList(PropsUtil.getWithDefault(KafkaToKuduSpecialExtractApp.PROP_PREFIX, "sendPhones", "").split(" *, *"))
			.stream().filter(phone -> phone != null && !"".equals(phone.trim())).collect(Collectors.toList());

	public static boolean sendMsg(List<String> phones, String... msgs) {
		boolean flag = false;
		if (!SEND_MSG || phones == null || phones.isEmpty() || msgs == null || msgs.length < 1) {
			return flag;
		}

		Map<String, String> headerMap = new HashMap<>();
		headerMap.put("Content-Type", "application/json");

		List<Map<String, Object>> msgList = new ArrayList<>();

		phones.forEach(phone -> {
			for (String msg : msgs) {
				log.info("send msg:{}, phone:{}", msg, phone);
				Map<String, Object> msgMap = new HashMap<>();
				msgMap.put("sourceSystem", "bigdataplatform");
				msgMap.put("logicTargetType", "mobile");
				msgMap.put("logicTarget", phone);
				msgMap.put("templateType", "X02");
				msgMap.put("sourceSystem", "bigdataplatform");
				Map<String, String> parameters = new HashMap<>();
				parameters.put("message", msg);
				msgMap.put("parameters", parameters);
				msgList.add(msgMap);
			}
		});

		try {
			String data = JSONObject.toJSONString(msgList, true);
			String requestId = UUID.randomUUID().toString();
			log.debug("send msg data:{}", data);
			String result = HttpUtils.doPost(SEND_MSG_URL + "?requestId=" + requestId, headerMap, data);
			log.debug("send msg ret:{}", result);
			flag = true;
		} catch (IOException e) {
			log.error("send msg error:{}" + e.getMessage(), e);
		}

		return flag;

	}

	public static boolean sendMsg(String... msgs) {
		return sendMsg(SEND_PHONES, msgs);
	}

	public static void main(String[] args) {

		System.out.println(SEND_PHONES);
		
		System.out.println(sendMsg("aaa", "bbb"));
	}
}
