package com.hncy58.kafka.consumer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.ServerStatusReportUtil;
import com.hncy58.heartbeat.HeartRunnable;
import com.hncy58.kafka.consumer.handler.HBaseHandler;
import com.hncy58.kafka.consumer.handler.Handler;
import com.hncy58.util.PropsUtil;

public class ConsumerToHBaseApp {

	private static final Logger log = LoggerFactory.getLogger(ConsumerToHBaseApp.class);

	private static final String PROP_PREFIX = "kafka-to-hbase";

	private static String agentSvrName = PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrName", "KafkaToHBase");
	private static String agentSvrGroup = PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrGroup", "KafkaToHBaseGroup");
	private static int agentSvrType = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrType", "2"));
	private static int agentSourceType = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "agentSourceType", "2"));
	private static int agentDestType = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "agentDestType", "2"));
	private static int svrHeartBeatSleepInterval = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "svrHeartBeatSleepInterval", "10"));
	private static int maxSvrStatusUpdateFailCnt = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "maxSvrStatusUpdateFailCnt", "2"));

	private static int svrRegFailSleepInterval = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "svrRegFailSleepInterval", "5"));

	private static int fetchMiliseconds = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "fetchMiliseconds", "1000"));
	private static int sleepSeconds = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "sleepSeconds", "5"));
	private static int minBatchSize = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "minBatchSize", "5000"));
	private static int minSleepCnt = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "minSleepCnt", "20"));
	private static int noDataMaxSleepCnt = Integer.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "noDataMaxSleepCnt", "5"));
	private static int maxOffsetCommitRetryCnt = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "maxOffsetCommitRetryCnt", "3"));
	private static int offsetCommitRetryInterval = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "offsetCommitRetryInterval", "3"));
	private static int maxErrHandledCnt = Integer
			.parseInt(PropsUtil.getWithDefault(PROP_PREFIX, "maxErrHandledCnt", "5"));
	private static int ERR_HANDLED_CNT = 0;
	private static Long TOTAL_MSG_CNT = 0L;

	private static String kafkaServers = PropsUtil.getWithDefault(PROP_PREFIX, "kafkaServers",
			"162.16.6.180:9092,162.16.6.181:9092,162.16.6.182:9092");
	private static String kafkaGroupId = PropsUtil.getWithDefault(PROP_PREFIX, "kafkaGroupId", "ConsumerToHBaseApp");
	private static List<String> subscribeToipcs = Arrays
			.asList(PropsUtil.getWithDefault(PROP_PREFIX, "subscribeToipcs", "").split(" *, *"));

	private static String localFileNamePrefix = PropsUtil.getWithDefault(PROP_PREFIX, "localFileNamePrefix",
			"unHadledData");
	private static String hbaseColumnFamilyName = PropsUtil.getWithDefault(PROP_PREFIX, "hbaseColumnFamilyName",
			"info");
	private static String zkServers = PropsUtil.getWithDefault(PROP_PREFIX, "zkServers",
			"162.16.6.180,162.16.6.181,162.16.6.182");
	private static String zkPort = PropsUtil.getWithDefault(PROP_PREFIX, "zkPort", "2181");

	private boolean run = false;
	private boolean shutdown_singal = false;
	public boolean shutdown = false;
	private static Thread heartThread;
	private static HeartRunnable heartRunnable;
	private static KafkaConsumer<String, String> consumer;
	private static Configuration hadoopConf = new Configuration(true);

	private Handler handler;

	public static void main(String[] args) {

		ConsumerToHBaseApp app = new ConsumerToHBaseApp();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				log.warn("开始运行进程退出钩子函数。");
				while (!app.getDownSignal()) {
					try {
						log.error("监测到中断进程信号，设置服务为下线！");
						app.setShutdown(true);
						Thread.sleep(2 * 1000);
					} catch (InterruptedException e) {
						log.error(e.getMessage(), e);
					}
				}
			}
		}, "ShutdownHookThread"));

		app.init(args);

		// 开始运行
		app.doRun(args);

		log.error("初始化服务失败，请检查相关配置是否正确！");
		app.setDownSignal(true);
		try {
			// 停止状态上报线程
			heartRunnable.setRun(false);
			heartRunnable.setSvrStatus(0);
			heartThread.interrupt();
			boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, 0,
					"监测到服务中断信号，退出服务！");
			log.info("设置服务状态为下线：" + ret);
			ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4, "设置服务状态为下线：" + ret
					+ "，shutdown_singal：" + app.getDownSignal() + "，ERR_HANDLED_CNT：" + ERR_HANDLED_CNT);
			log.info("上报告警结果：" + ret);
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		// Runtime.getRuntime().exit(2);
		// System.exit(2);
	}

	public void doRun(String[] args) {
		try {
			List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
			int sleepdCnt = 0;
			int noDataSleepdCnt = 0;

			while (run) {
				try {
					// 如果发送了终止进程消息，则停止消费，并且处理掉缓存的消息
					if (isShutdown() || ERR_HANDLED_CNT >= maxErrHandledCnt) {
						run = false;
						try {
							if (buffer != null && !buffer.isEmpty()) {
								doHandle(buffer);
								buffer.clear();
							}
							// 停止状态上报线程
							heartRunnable.setRun(false);
							heartRunnable.setSvrStatus(0);
							heartThread.interrupt();

							boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup,
									agentSvrType, 0, "监测到服务中断信号，退出服务！");
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
							System.exit(1);
						}
					} else {
						ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(fetchMiliseconds));
						int cnt = records.count();
						if (cnt > 0) {
							log.info("current polled " + cnt + " records.");
							TOTAL_MSG_CNT += cnt;
							log.info("total polled " + TOTAL_MSG_CNT + " records.");
							for (ConsumerRecord<String, String> record : records) {
								buffer.add(record);
							}

							if (buffer.size() >= minBatchSize || (sleepdCnt >= minSleepCnt && !buffer.isEmpty())) {
								sleepdCnt = 0;
								noDataSleepdCnt = 0;
								doHandle(buffer);
								buffer.clear();
//								Thread.sleep(500); //
							} else {
								log.info("current buffer remains " + buffer.size() + " records.");
								sleepdCnt += 1;
							}
						} else {
							log.info("no data to poll, sleep " + sleepSeconds + " s. buff size:" + buffer.size());
							if ((noDataSleepdCnt >= noDataMaxSleepCnt && !buffer.isEmpty())) {
								sleepdCnt = 0;
								noDataSleepdCnt = 0;
								doHandle(buffer);
								buffer.clear();
							} else {
								Thread.sleep(sleepSeconds * 1000);
								noDataSleepdCnt += 1;
							}
						}

						if (TOTAL_MSG_CNT >= Long.MAX_VALUE)
							TOTAL_MSG_CNT = 0L; // 达到最大值后重置为0
					}
				} catch (Exception e) {
					log.error("消费、处理数据异常:" + e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			log.error("主流程捕获到异常：" + e.getMessage(), e);
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}

	/**
	 * 初始化程序
	 * 
	 * @param args
	 */
	private void init(String[] args) {

		try {
			log.info("usage:" + ConsumerToHBaseApp.class.getName()
					+ " kafkaServers kafkaTopicGroupName kafkaToipcs FETCH_MILISECONDS MIN_BATCH_SIZE MIN_SLEEP_CNT SLEEP_SECONDS");
			log.info("eg:" + ConsumerToHBaseApp.class.getName()
					+ " localhost:9092 kafka_hdfs_group_2 test-topic-1 1000 5000 3 5");

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

			log.error("注册代理服务结果(-1:fail, 1:success, 2:standby) -> {}", ret);

			hadoopConf.set("dfs.support.append", "true");
			hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

			// for test
			// hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy",
			// "NEVER");
			// hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable",
			// "true");

			Properties props = new Properties();

			if (args.length > 0) {
				kafkaServers = args[0].trim();
			}

			if (args.length > 1) {
				kafkaGroupId = args[1].trim();
			}

			if (args.length > 2) {
				subscribeToipcs = Arrays.asList(args[2].trim().split(" *, *"));
			}

			if (args.length > 3) {
				hbaseColumnFamilyName = args[3].trim();
			}

			if (args.length > 4) {
				fetchMiliseconds = Integer.parseInt(args[4].trim());
			}

			if (args.length > 5) {
				minBatchSize = Integer.parseInt(args[5].trim());
			}

			if (args.length > 6) {
				minSleepCnt = Integer.parseInt(args[6].trim());
			}

			if (args.length > 7) {
				sleepSeconds = Integer.parseInt(args[7].trim());
			}
			
			if (args.length > 8) {
				noDataMaxSleepCnt = Integer.parseInt(args[8].trim());
			}

			props.put("bootstrap.servers", kafkaServers);
			props.put("group.id", kafkaGroupId);

			// props.put("auto.commit.interval.ms", "1000");
			props.put("enable.auto.commit", "false");
			props.put("isolation.level", "read_committed");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(subscribeToipcs);

			setHandler(new HBaseHandler(zkServers, zkPort, hbaseColumnFamilyName, localFileNamePrefix));

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

	private void doHandle(List<ConsumerRecord<String, String>> buffer) throws Exception {

		int offsetCommitRetryCnt = 0;
		boolean success = false;
		while (!success && offsetCommitRetryCnt < maxOffsetCommitRetryCnt) {
			try {
				getHandler().handle(buffer);
				success = true;
			} catch (Exception e) {
				log.error("处理数据异常，重试次数：" + offsetCommitRetryCnt + "，错误信息：" + e.getMessage(), e);
				Thread.sleep(offsetCommitRetryInterval * 1000);
			} finally {
				offsetCommitRetryCnt += 1;
			}
		}

		if (!success)
			ERR_HANDLED_CNT += 1;

		boolean cmtSuccess = commitOffsets();
		// 如果重试N次还是失败且偏移量提交成功，则记录到本地文件，然后发送告警信息到监控服务
		// 如果重试N次提交偏移量还是失败，则记录到本地文件，然后发送告警信息到监控服务
		if (!success && cmtSuccess) {
			success = doSaveToLocalFile(buffer);
		}

		if (!success) {
			log.error("经过重试写入未提交消息到本地文件失败，最终放弃，数据如下：{}", buffer);
		}
	}

	/**
	 * 可重试N次提交偏移量
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	private boolean commitOffsets() throws InterruptedException {
		int offsetCommitRetryCnt = 1;
		boolean success = false;
		while (!success && offsetCommitRetryCnt < maxOffsetCommitRetryCnt) {
			try {
				consumer.commitSync();
				success = true;
			} catch (Exception e) {
				log.error("消费成功数据，但提交偏移量失败，重试次数：" + offsetCommitRetryCnt + "，错误信息：" + e.getMessage(), e);
				Thread.sleep(offsetCommitRetryInterval * 1000);
			} finally {
				offsetCommitRetryCnt += 1;
			}
		}
		return success;
	}

	/**
	 * 重试N次保存未成功提交偏移量的数据
	 * 
	 * @param buffer
	 * @return
	 * @throws InterruptedException
	 */
	private boolean doSaveToLocalFile(List<ConsumerRecord<String, String>> buffer) throws InterruptedException {

		int offsetCommitRetryCnt = 1;
		boolean success = false;
		while (!success && offsetCommitRetryCnt < maxOffsetCommitRetryCnt) {
			try {
				getHandler().onHandleFail(buffer);
				success = true;
			} catch (Exception e) {
				log.error("写入未处理数据到本地文件失败，重试次数：" + offsetCommitRetryCnt + "，错误信息：" + e.getMessage(), e);
				Thread.sleep(offsetCommitRetryInterval * 1000);
			} finally {
				offsetCommitRetryCnt += 1;
			}
		}
		return success;
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

	public void setShutdown(boolean shutdown) {
		this.shutdown = shutdown;
	}

	public Handler getHandler() {
		return handler;
	}

	public void setHandler(Handler handler) {
		this.handler = handler;
	}

}
