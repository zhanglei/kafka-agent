package com.hncy58.kafka;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.hncy58.ds.DSPoolUtil;
import com.hncy58.ds.DataSourceUtil;

public class DSTest {
	public static void main(String[] args) throws SQLException {
		
//		String driverClass = "com.mysql.jdbc.Driver";
//		String connectURI = "jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
//		String username = "tokings";
//		String password = "19890625";
//		int initialSize = 5;
//		int maxTotal = 20;
//		int maxIdle = 3;
//		int maxWaitMillis = 6000;
//		DataSourceUtil.initDataSource(connectURI, username, password, driverClass, initialSize, maxTotal, maxIdle, maxWaitMillis);
//		Connection con = DataSourceUtil.getConnection();
//		ResultSet rs = con.createStatement().executeQuery("select now() from dual");
//		while(rs.next()) {
//			System.out.println(rs.getObject(1));
//		}
//		DataSourceUtil.shutdownDataSource();
		
//		System.out.println(DSPoolUtil.update("insert into agent_svr_info(id, svr_name, svr_group, svr_type, status, create_time, update_time) values(?,?,?,?,?,now(), now()) ", null, "testname", "testgrp", 2, 1));
//		String updateSql = "update agent_svr_info "
//				+ "set status = ?, update_time = ? "
//				+ "where svr_name = ? and svr_group = ? and svr_type = ? ";
//		int ret = DSPoolUtil.update(updateSql, 2, new Date(), "testname", "testgrp", 2);
//		System.out.println(ret);
		
		List<Map<String, Object>> data = DSPoolUtil.query("select * from agent_svr_info", null);
		System.out.println(data);
		
		String updateSql = "update agent_svr_info "
				+ "set status = ?, update_time = now() "
				+ "where svr_name = ? and svr_group = ? and svr_type = ? ";
		
		List<List<Object>> paramsList = new ArrayList<>();
		paramsList.add(Arrays.asList(1, "testname", "testgrp", 2));
		paramsList.add(Arrays.asList(1, "testname1", "testgrp", 2));
		paramsList.add(Arrays.asList(1, "testname123", "testgrp123", 2));
		
		int[] ret = DSPoolUtil.batchUpdate(updateSql, paramsList);
		System.out.println(Arrays.asList(ret));
		
		DSPoolUtil.shutdownDataSource();
	}
}
