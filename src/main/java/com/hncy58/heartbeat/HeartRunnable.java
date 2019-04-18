package com.hncy58.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.ServerStatusReportUtil;

public class HeartRunnable implements Runnable {

	private static final Logger log = LoggerFactory.getLogger("abc");
	
	private int sleepInterval = 10;
	
	private String agentSvrName;
	private String agentSvrGroup;
	private int agentSvrType = 2;
	private int agentSourceType = 2;
	private int agentDestType = 2;
	private int svrStatus = 1;
	
	private boolean run = false;

	public boolean isRun() {
		return run;
	}

	public void setRun(boolean run) {
		this.run = run;
	}

	public HeartRunnable() {
		super();
	}

	public HeartRunnable(String agentSvrName, String agentSvrGroup, int agentSvrType, int agentSourceType,
			int agentDestType, int sleepInterval) {
		super();
		this.agentSvrName = agentSvrName;
		this.agentSvrGroup = agentSvrGroup;
		this.agentSvrType = agentSvrType;
		this.agentSourceType = agentSourceType;
		this.agentDestType = agentDestType;
		this.sleepInterval = sleepInterval;
	}

	public String getAgentSvrName() {
		return agentSvrName;
	}

	public void setAgentSvrName(String agentSvrName) {
		this.agentSvrName = agentSvrName;
	}

	public String getAgentSvrGroup() {
		return agentSvrGroup;
	}

	public void setAgentSvrGroup(String agentSvrGroup) {
		this.agentSvrGroup = agentSvrGroup;
	}

	public int getAgentSvrType() {
		return agentSvrType;
	}

	public void setAgentSvrType(int agentSvrType) {
		this.agentSvrType = agentSvrType;
	}

	public int getAgentSourceType() {
		return agentSourceType;
	}

	public void setAgentSourceType(int agentSourceType) {
		this.agentSourceType = agentSourceType;
	}

	public int getAgentDestType() {
		return agentDestType;
	}

	public void setAgentDestType(int agentDestType) {
		this.agentDestType = agentDestType;
	}

	public int getSvrStatus() {
		return svrStatus;
	}

	public void setSvrStatus(int svrStatus) {
		this.svrStatus = svrStatus;
	}

	@Override
	public void run() {
		run = true;
		while(run) {
			try {
				Thread.sleep(sleepInterval * 1000);
				boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, getSvrStatus(), "心跳上报");
				log.warn("update agent svr status -> {}", ret);
			} catch (Exception e) {
				log.error("update agent svr status error:" + e.getMessage(), e);
				run = false;
			}
		}
	}

}
