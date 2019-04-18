package com.hncy58.ds;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.util.Utils;

public class ServerStatusReportUtil {

	static transient final Logger log = LoggerFactory.getLogger(ServerStatusReportUtil.class);

	/**
	 * 向监控中心注册服务 <br>
	 * 
	 * @param agentSvrName
	 * @param agentSvrGroup
	 * @param agentSvrType
	 * @param agentSourceType
	 * @param agentDestType
	 * @return 注册结果状态 <br>
	 *         1：成功，2：备用，其他：
	 */
	public static int register(String agentSvrName, String agentSvrGroup, int agentSvrType, int agentSourceType,
			int agentDestType, int heartBeatSleepInterval, int maxSvrStatusUpdateFailCnt) {

		String groupQuerySql = "select t.id, status, heartbeat_interval, max_heartbeat_fail_cnt"
				+ ", UNIX_TIMESTAMP(now()) - UNIX_TIMESTAMP(t.update_time) second_diff "
				+ "from agent_svr_info t where t.svr_group = ? and t.svr_type = ? and t.status = ? ";

		String querySql = "select 1 from agent_svr_info t where t.svr_name = ? and t.svr_group = ? and t.svr_type = ?";

		String insertSql = "insert into agent_svr_info(id, svr_name, svr_group, svr_type, source_type, dest_type, status,heartbeat_interval"
				+ ", max_heartbeat_fail_cnt, remark, create_time, update_time) values(?,?,?,?,?,?,?,?,?,?,now(), now()) ";

		String updateSql = "update agent_svr_info set status = ?, heartbeat_interval=?, max_heartbeat_fail_cnt=?, remark=?"
				+ ", update_time = now() where svr_name = ? and svr_group = ? and svr_type = ?";

		// 允许最多心跳更新失败次数
		// int maxSvrStatusUpdateFailCnt = 5;

		try {
			List<Map<String, Object>> groupRs = DSPoolUtil.query(groupQuerySql, agentSvrGroup, agentSvrType, 1);
			// 如果已经有同组的服务注册且在正常运行当中，则启用为备用服务
			int svrStatus = 1;
			String remark = "主服务";
			if (!groupRs.isEmpty()) {
				remark = "备用服务";
				svrStatus = 2;
				List<Integer> updateIds = new ArrayList<>();
				groupRs.forEach(map -> {
					int heartbeat_interval = Utils.toInt(map.get("heartbeat_interval"));
					int max_heartbeat_fail_cnt = Utils.toInt(map.get("max_heartbeat_fail_cnt"));
					int second_diff = Utils.toInt(map.get("second_diff"));

					if (second_diff >= heartbeat_interval * max_heartbeat_fail_cnt) {
						updateIds.add(Integer.parseInt(map.get("id").toString()));
					}
				});

				if (!updateIds.isEmpty()) {
					List<Object> params = new ArrayList<>();
					params.add(0);
					StringBuffer updateToDownSql = new StringBuffer(
							"update agent_svr_info set status = ?, update_time = now() where id in( ");
					updateIds.forEach(id -> updateToDownSql.append("?,"));
					updateToDownSql.append("?)");
					params.addAll(updateIds);
					params.add(-1);
					int ret = DSPoolUtil.update(updateToDownSql.toString(), params.toArray());
					log.info("update svr status to down, ret:{}, ids:{}", ret, updateIds);
				}
			}

			List<Map<String, Object>> rs = DSPoolUtil.query(querySql, agentSvrName, agentSvrGroup, agentSvrType);
			if (rs != null && !rs.isEmpty()) {
				int ret = DSPoolUtil.update(updateSql, svrStatus, heartBeatSleepInterval, maxSvrStatusUpdateFailCnt,
						remark, agentSvrName, agentSvrGroup, agentSvrType);
				return ret > 0 ? svrStatus : -1;
			} else {
				int ret = DSPoolUtil.update(insertSql, null, agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
						agentDestType, svrStatus, heartBeatSleepInterval, maxSvrStatusUpdateFailCnt, remark);
				return ret > 0 ? svrStatus : -1;
			}
		} catch (SQLException e) {
			log.error("注册失败，" + e.getMessage(), e);
		}

		return -1;
	}

	public static boolean reportSvrStatus(String agentSvrName, String agentSvrGroup, int agentSvrType, int status,
			String remark) throws SQLException {
		String updateSql = "update agent_svr_info set status = ?, remark = ?, update_time = now() where svr_name = ? and svr_group = ? and svr_type = ?";
		int ret = DSPoolUtil.update(updateSql, status, remark, agentSvrName, agentSvrGroup, agentSvrType);
		return ret > 0;
	}

	public static boolean reportAlarm(String agentSvrName, String agentSvrGroup, int agentSvrType, int alarm_type,
			int alarm_level, String remark) throws SQLException {
		String sql = "insert into agent_svr_alarm(id, svr_name, svr_group, svr_type, alarm_type, alarm_level, status, remark, create_time, update_time) values(?,?,?,?,?,?,?,?,now(), now()) ";
		int ret = DSPoolUtil.update(sql, null, agentSvrName, agentSvrGroup, agentSvrType, alarm_type, alarm_level, 0,
				remark);
		return ret > 0;
	}
}
