package com.hncy58.kafka.monitor;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.DSPoolUtil;
import com.hncy58.ds.ServerStatusReportUtil;
import com.hncy58.heartbeat.HeartRunnable;
import com.hncy58.util.PropsUtil;

/**
 * kafka主题消费组偏移量监控
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年10月25日 下午4:24:52
 *
 */
public class KafkaTopicGroupOffsetsMonitor {

	private static final Logger log = LoggerFactory.getLogger(KafkaTopicGroupOffsetsMonitor.class);

	private static final String PROP_PREFIX = "kafka-grp-monitor";

	private static String agentSvrName = PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrName", "kafkaTopicMonitor");
	private static String agentSvrGroup = PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrGroup",
			"kafkaTopicMonitorGroup");
	private static int agentSvrType = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrType", "2"));
	private static int agentSourceType = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "agentSourceType", "0"));
	private static int agentDestType = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "agentDestType", "0"));

	private static int svrHeartBeatSleepInterval = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "svrHeartBeatSleepInterval", "10"));
	private static int maxSvrStatusUpdateFailCnt = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "maxSvrStatusUpdateFailCnt", "2"));

	private static int svrRegFailSleepInterval = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "svrRegFailSleepInterval", "5"));

	Properties kafkaConsumerProps = new Properties();
	private String kafkaServers = PropsUtil.getWithDefault(PROP_PREFIX, "kafkaServers",
			"162.16.6.180:9092,162.16.6.181:9092,162.16.6.182:9092");
	private int fetchInterval = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "fetchInterval", "30"));
	boolean run = false;

	private static boolean shutdown_singal = false;
	public static boolean shutdown = false;

	private static Thread heartThread;
	private static HeartRunnable heartRunnable;
	private int ERR_HANDLED_CNT = 0;
	private static int MAX_ERR_HANDLED_CNT = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "MAX_ERR_HANDLED_CNT", "5"));

	static final String QUERY_ALL_SQL = "SELECT * FROM kafka_topic_grp_cfg WHERE status = 1";

	static final String QUERY_MONITOR_SQL = "SELECT * FROM kafka_topic_grp_monitor WHERE date_id=?";

	static final String UPDATE_SQL = "UPDATE kafka_topic_grp_monitor SET start_offset=?, curr_offset=?, latest_offset=?, update_time=NOW() "
			+ " WHERE topic_name=? AND partition_id=? AND grp_name=? AND date_id=?";

	static final String INSERT_SQL = "INSERT INTO kafka_topic_grp_monitor(id,date_id,topic_name,partition_id,grp_name"
			+ ",start_offset,curr_offset,latest_offset,create_time,update_time) VALUES(?,?,?,?,?,?,?,?,NOW(),NOW())";

	static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	static int hookSleepCnt = 0;
	static int hookMaxSleepCnt = 10;

	public static void main(String[] args) {

		System.out.println("Uage:\n\t" + KafkaTopicGroupOffsetsMonitor.class.getName() + " kafkaServers fetchInterval");
		System.out.println("eg:\n\t" + KafkaTopicGroupOffsetsMonitor.class.getName() + " 192.168.144.128:9092 5");

		KafkaTopicGroupOffsetsMonitor app = new KafkaTopicGroupOffsetsMonitor();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				log.warn("开始运行进程退出钩子函数。");
				int maxCnt = 10;
				int cnt = 0;
				while (!app.getDownSignal()) {
					try {
						if (cnt >= maxCnt) {
							// 停止状态上报线程
							if(heartRunnable != null) {
								heartRunnable.setRun(false);
								heartRunnable.setSvrStatus(0);
								heartThread.interrupt();
							}
							boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup,
									agentSvrType, 0, "监测到服务中断信号，退出服务！");
							log.info("设置服务状态为下线：" + ret);
							ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
									"设置服务状态为下线：" + ret + "，shutdown_singal：" + app.getDownSignal());
							log.info("上报告警结果：" + ret);
							app.setShutdown(true);
							app.setDownSignal(true);
							log.error("监测到服务中断信号，退出服务！");
							Runtime.getRuntime().exit(0);
							System.exit(0);
							break;
						}
						app.setShutdown(true);
						log.error("监测到中断进程信号，设置服务为下线！" + app.isShutdown());
						Thread.sleep(2 * 1000);
						cnt++;
					} catch (Exception e) {
						log.error(e.getMessage(), e);
					}
				}
			}
		}, "ShutdownHookThread"));

		app.init(args);
		app.doRun();
		log.error("初始化服务失败，请检查相关配置是否正确！");
		app.setDownSignal(true);
		try {
			// 停止状态上报线程
			if(heartRunnable != null) {
				heartRunnable.setRun(false);
				heartRunnable.setSvrStatus(0);
				heartThread.interrupt();
			}
			boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, 0,
					"监测到服务中断信号，退出服务！");
			log.info("设置服务状态为下线：" + ret);
			ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4, "设置服务状态为下线：" + ret
					+ "，shutdown_singal：" + app.getDownSignal());
			log.info("上报告警结果：" + ret);
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		// Runtime.getRuntime().exit(2);
		// System.exit(2);
	}

	private void doRun() {

		while (run) {
			// 如果发送了终止进程消息，则停止，并且处理掉缓存的消息
			if (isShutdown() || ERR_HANDLED_CNT >= MAX_ERR_HANDLED_CNT) {
				run = false;
				try {
					// 停止状态上报线程
					heartRunnable.setRun(false);
					heartRunnable.setSvrStatus(0);
					heartThread.interrupt();

					boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, 0,
							"监测到服务中断信号，退出服务！");
					log.info("设置服务状态为下线：" + ret);

					ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
							"设置服务状态为下线：" + ret + "，shutdown_singal：" + shutdown_singal + "，ERR_HANDLED_CNT："
									+ ERR_HANDLED_CNT);
					log.info("上报告警结果：" + ret);

					setShutdown(true);
					setDownSignal(true);
					log.error("监测到服务中断信号，退出服务！");
					Runtime.getRuntime().exit(0);
					System.exit(0);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					log.error("捕获到异常停止状态，直接退出进程！");
					setShutdown(true);
					setDownSignal(true);
					Runtime.getRuntime().exit(0);
					System.exit(1);
				}
				break;
			}

			List<Map<String, Object>> topicGroups = null;
			final List<Map<String, Object>> monitorTopicGroups = new ArrayList<>();
			int dateId = Integer.parseInt(sdf.format(new Date()));
			try {
				topicGroups = DSPoolUtil.query(QUERY_ALL_SQL, new Object[] {});
				monitorTopicGroups.addAll(DSPoolUtil.query(QUERY_MONITOR_SQL, dateId));
			} catch (SQLException e1) {
				log.error("获取主题消费组配置信息失败，" + e1.getMessage(), e1);
			}

			if (topicGroups != null && !topicGroups.isEmpty()) {

				topicGroups.forEach(topicGroup -> {
					String topic = topicGroup.get("topic_name").toString();
					String group = topicGroup.get("grp_name").toString();
					log.debug("start monitor, grp:{},topic:{}", group, topic);

					kafkaConsumerProps.put("group.id", group);
					KafkaConsumer<String, String> consumer = null;
					List<List<Object>> insertParamsList = new ArrayList<>();
					List<List<Object>> updateParamsList = new ArrayList<>();
					HashMap<String, List<Object>> insertMap = new HashMap<String, List<Object>>();
					HashMap<String, List<Object>> updateMap = new HashMap<String, List<Object>>();

					try {
						consumer = new KafkaConsumer<>(kafkaConsumerProps);
						Set<TopicPartition> tps = new HashSet<>();
						consumer.partitionsFor(topic)
								.forEach(pi -> tps.add(new TopicPartition(pi.topic(), pi.partition())));

						consumer.assign(tps);

						for (TopicPartition tp : tps) {
							if (existInMonitorTbl(monitorTopicGroups, group, tp.topic(), tp.partition())) {
								updateMap.put(tp.toString(), new ArrayList<>());
							} else {
								insertMap.put(tp.toString(), new ArrayList<>());
							}
						}

						consumer.beginningOffsets(tps).forEach((tp, offset) -> {
							List<Object> params = new ArrayList<>();
							if (insertMap.containsKey(tp.toString())) {
								params = insertMap.get(tp.toString());
								params.add(null);
								params.add(dateId);
								params.add(tp.topic());
								params.add(tp.partition());
								params.add(group);
								// start_offset
								params.add(offset);
							} else if (updateMap.containsKey(tp.toString())) {
								params = updateMap.get(tp.toString());
								// start_offset
								params.add(offset);
							}
						});

						for (TopicPartition tp : tps) {
							OffsetAndMetadata metaData = consumer.committed(tp);
							Long offset = null;
							if (metaData != null)
								offset = metaData.offset();
							if (insertMap.containsKey(tp.toString())) {
								// current_offset
								insertMap.get(tp.toString()).add(offset);
							} else if (updateMap.containsKey(tp.toString())) {
								// current_offset
								updateMap.get(tp.toString()).add(offset);
							}
						}

						consumer.endOffsets(tps).forEach((tp, offset) -> {
							if (insertMap.containsKey(tp.toString())) {
								// end_offset
								insertMap.get(tp.toString()).add(offset);
							} else if (updateMap.containsKey(tp.toString())) {
								updateMap.get(tp.toString()).add(offset);
								updateMap.get(tp.toString()).add(tp.topic());
								updateMap.get(tp.toString()).add(tp.partition());
								updateMap.get(tp.toString()).add(group);
								// end_offset
								updateMap.get(tp.toString()).add(dateId);
							}
						});

						insertParamsList = new ArrayList<>(insertMap.values());
						updateParamsList = new ArrayList<>(updateMap.values());
						log.info("insert size:{}", insertParamsList.size());
						log.info("update size:{}", updateParamsList.size());

						if (!insertParamsList.isEmpty()) {
							int[] inserted = DSPoolUtil.batchUpdate(INSERT_SQL, insertParamsList);
							log.info("insert ret:{}", inserted[0]);
						}

						if (!updateParamsList.isEmpty()) {
							int[] updated = DSPoolUtil.batchUpdate(UPDATE_SQL, updateParamsList);
							log.info("update ret:{}", updated[0]);
						}

					} catch (Exception e) {
						log.error(e.getMessage(), e);
						run = false;
					} finally {
						if (consumer != null)
							consumer.close();
					}
				});
			}

			try {
				Thread.sleep(fetchInterval * 1000);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
		}

	}

	private boolean existInMonitorTbl(List<Map<String, Object>> monitorTopicGroups, String group, String topic,
			int partition) {
		String source = group + topic + partition;
		String dest = "";
		for (Map<String, Object> data : monitorTopicGroups) {
			dest = data.get("grp_name").toString() + data.get("topic_name") + data.get("partition_id");
			if (source.equals(dest)) {
				return true;
			}
		}
		return false;
	}

	private void init(String[] args) {

		try {
			if (args.length > 0) {
				kafkaServers = args[0].trim();
			}

			if (args.length > 1) {
				fetchInterval = Integer.valueOf(args[1].trim());
			}

			kafkaConsumerProps.put("bootstrap.servers", kafkaServers);
			kafkaConsumerProps.put("enable.auto.commit", "false");
			kafkaConsumerProps.put("isolation.level", "read_committed");
			kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

			// for test
			kafkaConsumerProps.put("request.timeout.ms", "50000");
			kafkaConsumerProps.put("auto.commit.interval.ms", "1000");
			kafkaConsumerProps.put("max.partition.fetch.bytes", "10000000");
			kafkaConsumerProps.put("heartbeat.interval.ms", "25000");
			kafkaConsumerProps.put("session.timeout.ms", "30000");

			int ret = ServerStatusReportUtil.register(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
					agentDestType, svrHeartBeatSleepInterval, maxSvrStatusUpdateFailCnt);

			while (ret != 1) {
				log.error("注册服务失败，name:{}, group:{}, svrType:{}, sourceType:{}, destType:{}, 注册结果:{}", agentSvrName,
						agentSvrGroup, agentSvrType, agentSourceType, agentDestType, ret);
				try {
					Thread.sleep(svrRegFailSleepInterval * 1000);
				} catch (InterruptedException e) {
					log.error(e.getMessage(), e);
				}
				ret = ServerStatusReportUtil.register(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
						agentDestType, svrHeartBeatSleepInterval, maxSvrStatusUpdateFailCnt);
			}

			log.info("注册代理服务结果(-1:fail, 1:success, 2:standby) -> {}", ret);

			heartRunnable = new HeartRunnable(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType, agentDestType,
					svrHeartBeatSleepInterval);
			heartThread = new Thread(heartRunnable, "agentSvrStatusReportThread");
			heartThread.start();

			log.info("启动代理服务状态定时上报线程:" + heartThread.getName());

			run = true;
		} catch (Exception e) {
			log.error("初始化参数失败，" + e.getMessage(), e);
		}
	}

	public void setDownSignal(boolean flag) {
		shutdown_singal = flag;
	}

	public boolean getDownSignal() {
		return shutdown_singal;
	}

	public boolean isShutdown() {
		return shutdown;
	}

	public void setShutdown(boolean flag) {
		shutdown = flag;
	}

}
