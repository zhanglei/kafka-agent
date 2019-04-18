package com.hncy58.hbase;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.DSPoolUtil;
import com.hncy58.ds.ServerStatusReportUtil;
import com.hncy58.util.PropsUtil;
import com.hncy58.util.Utils;

public class HBaseBatchDeleteScheduleApp implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(HBaseBatchDeleteScheduleApp.class);

	private static final String PROP_PREFIX = "hbase-tabledata-delete";
	private static String agentSvrName = PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrName", "hbaseTableDataDelete");
	private static String agentSvrGroup = PropsUtil.getWithDefault(PROP_PREFIX, "agentSvrGroup",
			"hbaseTableDataDeleteGroup");
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

	private static final String QUERY_SQL = "select * from hbase_tbl_batch_del_cfg where status = 1 ";
	private static final String INSERT_SQL = "insert into hbase_tbl_batch_del_audit"
			+ "(id,db_name,table_name,status,start_time,end_time,del_size,create_time,remark) values(?,?,?,?,?,?,?,now(),?)";

	private static String zkServers = PropsUtil.getWithDefault(PROP_PREFIX, "zkServers", "192.168.144.128");
	private static String zkPort = PropsUtil.getWithDefault(PROP_PREFIX, "zkPort", "2181");

	private static int maxDeleteBatch = 20000;

	private static ScheduledExecutorService service = Executors.newScheduledThreadPool(2);
	private static boolean shutdown_singal = false;
	public static boolean shutdown = false;

	public static void setDownSignal(boolean flag) {
		shutdown_singal = flag;
	}

	public static boolean getDownSignal() {
		return shutdown_singal;
	}

	public static boolean isShutdown() {
		return shutdown;
	}

	public static void setShutdown(boolean flag) {
		shutdown = flag;
	}

	public HBaseBatchDeleteScheduleApp() {
		super();
	}

	public static void main(String[] args) throws Exception {

		init(args);

		HBaseBatchDeleteScheduleApp app = new HBaseBatchDeleteScheduleApp();

		log.info("Usage:\n" + HBaseBatchDeleteScheduleApp.class.getName() + " zkServers zkPort maxDeleteBatch");

		long delay = 0;
		Calendar c = Calendar.getInstance();
		long start = c.getTimeInMillis();
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) + 1);
		long end = c.getTimeInMillis();

		// for test
		// service.scheduleAtFixedRate(app, delay, 120, TimeUnit.SECONDS);

		// 每天凌晨30分开始调度
		delay = end - start + 30 * 60 * 1000;
		service.scheduleAtFixedRate(app, delay, 1, TimeUnit.DAYS);

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				log.error("开始运行进程退出钩子函数。");
				while (!getDownSignal()) {
					try {
						log.error("监测到中断进程信号，设置服务为下线！");
						setShutdown(true);
						Thread.sleep(2 * 1000);
					} catch (InterruptedException e) {
						log.error(e.getMessage(), e);
					}
				}
			}
		}, "ShutdownHookThread"));

		while (true) {
			Thread.sleep(10 * 1000);
			// 如果发送了终止进程消息，则停止，并且处理掉缓存的消息
			if (isShutdown()) {
				try {
					setShutdown(true);
					setDownSignal(true);

					boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, 0,
							"监测到服务中断信号，退出服务！");
					log.error("设置服务状态为下线：" + ret);

					ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
							"设置服务状态为下线：" + ret + "，shutdown_singal：" + shutdown_singal);
					log.error("上报告警结果：" + ret);

					// 停止调度
					boolean suc = service.awaitTermination(10, TimeUnit.SECONDS);
					if (!suc) {
						service.shutdown();
					}

					log.error("监测到服务中断信号，退出服务！");
					Runtime.getRuntime().exit(0);
					System.exit(0);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					log.error("捕获到异常停止状态，直接退出进程！");
					System.exit(1);
				}
				break;
			}

			boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, 1, "心跳上报");
			log.info("update agent svr status -> {}", ret);
		}
	}

	private static void doDelete(List<Map<String, Object>> tables) {

		tables.forEach(rowMap -> {
			String db = Utils.toString(rowMap.get("db_name"));
			String table = Utils.toString(rowMap.get("table_name"));

			Calendar c = Calendar.getInstance();
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);

			int startDaysFromNow = Utils.toInt(rowMap.get("start_days_from_now"));
			int deleteDays = Utils.toInt(rowMap.get("delete_days"));

			c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) - startDaysFromNow);
			Long end = c.getTimeInMillis();
			c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) - deleteDays);
			Long start = c.getTimeInMillis();

			if (Utils.isEmpty(table)) {
				log.error("需要删除的表没有配置正确，请检查！{}", rowMap);
				return;
			}

			deleteTimeRange(db, table, start, end);
		});
	}

	private static List<Map<String, Object>> getDeleteTables() throws SQLException {
		return DSPoolUtil.query(QUERY_SQL, new Object[] {});
	}

	/**
	 * 删除一段时间的表记录
	 *
	 * @param c
	 * @param minTime
	 * @param maxTime
	 */
	private static void deleteTimeRange(String db, String tableName, Long minTime, Long maxTime) {

		String realTable = (Utils.isEmpty(db)) ? tableName : db + ":" + tableName;
		Table table = null;
		Connection connection = null;
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", zkServers);
		hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort);
		hbaseConf.set("hbase.defaults.for.version.skip", "true");
		long cnt = 0;

		try {
			connection = ConnectionFactory.createConnection(hbaseConf);
			Scan scan = new Scan();
			scan.setTimeRange(minTime, maxTime);
			table = connection.getTable(TableName.valueOf(realTable));

			long start = System.currentTimeMillis();
			ResultScanner rs = table.getScanner(scan);
			List<Delete> list = getDeleteList(rs);
			log.info("scan table " + realTable + " total used " + (System.currentTimeMillis() - start) + " ms.");
			log.info("scan table " + realTable + " size -> " + list.size());

			if (list.isEmpty()) {
				log.warn("table " + realTable + " has no data to delete. ignore it.");
				DSPoolUtil.update(INSERT_SQL, null, db, tableName, 1, new Date(minTime), new Date(maxTime), list.size(),
						"success");
				return;
			}

			Iterator<Delete> delIt = list.iterator();
			List<Delete> batchDeletes = new ArrayList<>();

			while (delIt.hasNext()) {
				batchDeletes.add(delIt.next());
				if (batchDeletes.size() >= maxDeleteBatch) {
					start = System.currentTimeMillis();
					table.delete(batchDeletes);
					log.info("batch delete used " + (System.currentTimeMillis() - start) + " ms. batch size -> "
							+ batchDeletes.size());

					cnt += batchDeletes.size();
					batchDeletes.clear();
				}
			}

			if (!batchDeletes.isEmpty()) {
				start = System.currentTimeMillis();
				table.delete(batchDeletes);
				log.info("batch delete used " + (System.currentTimeMillis() - start) + " ms. batch size -> "
						+ batchDeletes.size());
			}

			log.info("hbase table {} delete finished. start:{},end:{},total:{}", realTable, minTime, maxTime,
					list.size());
			DSPoolUtil.update(INSERT_SQL, null, db, tableName, 1, new Date(minTime), new Date(maxTime), list.size(),
					"success");
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			try {
				DSPoolUtil.update(INSERT_SQL, null, db, tableName, 1, new Date(minTime), new Date(maxTime), cnt,
						"fail");
				ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 3, "删除数据失败，db:" + db
						+ ",table:" + tableName + ",min:" + minTime + " , max:。" + maxTime + e.getMessage());
			} catch (SQLException e1) {
				log.error(e1.getMessage(), e1);
			}
		} finally {
			if (null != table) {
				try {
					table.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
			}

			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
			}
		}
	}

	private static List<Delete> getDeleteList(ResultScanner rs) {

		List<Delete> list = new ArrayList<>();
		try {
			for (Result r : rs) {
				Delete d = new Delete(r.getRow());
				list.add(d);
			}
		} finally {
			rs.close();
		}
		return list;
	}

	@Override
	public void run() {
		try {
			List<Map<String, Object>> tables = getDeleteTables();

			if (!tables.isEmpty())
				doDelete(tables);

			log.info("delete finished. tables:" + tables);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	private static void init(String[] args) {
		try {
			if (args.length > 0) {
				zkServers = args[0].trim();
			}

			if (args.length > 1) {
				zkPort = args[1].trim();
			}

			if (args.length > 2) {
				maxDeleteBatch = Integer.parseInt(args[2].trim());
			}
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
		} catch (Exception e) {
			log.error("初始化参数失败，" + e.getMessage(), e);
		}
	}
}
