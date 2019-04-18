package com.hncy58.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hncy58.ds.DataSourceUtil;

public class TiDBUtil {

	private static boolean inited = false;
	private static Map<String, Map<String, String>> tableSchemas = new HashMap<>();

	private static DataSourceUtil dsUtil;
	private static String driverClass = "com.mysql.jdbc.Driver";

	public static void main(String[] args) throws SQLException {

		String url = "jdbc:mysql://162.16.9.238:4000/?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
		String username = "root";
		String password = "";
		int initialSize = 5;
		int maxTotal = 10;
		int maxIdle = 5;
		int maxWaitMillis = 5 * 1000;
		
		initDB(url, username, password, initialSize, maxTotal, maxIdle, maxWaitMillis);

		System.out.println(tableSchemas);

		if (inited) {
			batchInTransaction(Arrays.asList("update test.ccs_order set org = 'test1' where order_id = 628193",
					"update test.ccs_order set org = 'test2' where order_id = 628195",
					"update test.ccs_loan set org = 'test1' where loan_id = 45",
					"update test.ccs_loan set org = 'test2' where loan_id = 47"));
		}

		dsUtil.shutdownDataSource();
	}

	public static boolean isInited() {
		return inited;
	}

	public static boolean initDB(String url, String username, String password, int initialSize, int maxTotal,
			int maxIdle, int maxWaitMillis) throws SQLException {

		dsUtil = new DataSourceUtil();
		dsUtil.initDataSource(url, username, password, driverClass, initialSize, maxTotal, maxIdle, maxWaitMillis);

		Connection conn = getConnection();
		Set<String> excludeDBs = new HashSet<>(
				Arrays.asList("INFORMATION_SCHEMA", "tidb_loader", "mysql", "PERFORMANCE_SCHEMA"));
		Set<String> excludeTables = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
		List<String> dbs = new ArrayList<>();
		List<String> tables = new ArrayList<>();

		try {
			Statement st1 = conn.createStatement();
			String sql = "show databases";

			ResultSet rs1 = st1.executeQuery(sql);

			while (rs1.next()) {
				if (excludeDBs.contains(rs1.getString(1))) {
					continue;
				}
				dbs.add(rs1.getString(1));
			}

			rs1.close();
			st1.close();

			// System.out.println(dbs);

			for (String db : dbs) {
				PreparedStatement st2 = conn.prepareStatement(sql);
				st2.addBatch("use " + db); // 必须先使用数据库切换，然后才能执行，且必须为addBatch
				st2.addBatch("show tables");
				st2.executeBatch();
				ResultSet rs2 = st2.getResultSet();
				while (rs2.next()) {
					if (excludeTables.contains(rs2.getString(1))) {
						continue;
					}
					tables.add(db + "." + rs2.getString(1));
				}
			}

			// System.out.println(tables);

			for (String table : tables) {
				Map<String, String> tableColumns = new HashMap<>();
				PreparedStatement st = null;
				String tableSql = "SELECT * FROM " + table + " limit 0";
				st = conn.prepareStatement(tableSql);
				st.execute(); // 此步骤在mysql数据库下不休要，但在tidb下必须执行才能获取元数据信息
				ResultSetMetaData md = st.getMetaData();
				int size = md.getColumnCount();
				for (int i = 0; i < size; i++) {
					tableColumns.put(md.getColumnName(i + 1).toUpperCase(), md.getColumnTypeName(i + 1));
				}

				tableSchemas.put(table, tableColumns);
			}

			// System.out.println(tableSchemas);
			inited = true;
		} catch (SQLException e) {
			inited = false;
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

		return inited;
	}

	public static int[] batchInTransaction(List<String> sqls) throws SQLException {

		if (sqls == null || sqls.isEmpty()) {
			return null;
		}
		boolean autoCommit = true;
		Connection conn = null;
		try {
			conn = dsUtil.getConnection();
			autoCommit = conn.getAutoCommit();
			// 关闭自动提交，即开启事务
			conn.setAutoCommit(false);
			long start = System.currentTimeMillis();
			Statement st = conn.createStatement();
			for (int i = 0; i < sqls.size(); i++) {
				st.addBatch(sqls.get(i));
			}
			int[] rets = st.executeBatch();
			// 提交事务
			conn.commit();
			System.out.println("used " + (System.currentTimeMillis() - start) + " ms.");
			return rets;
		} catch (SQLException e) {
			conn.rollback();
			throw e;
		} finally {
			if (conn != null) {
				conn.setAutoCommit(autoCommit);
				conn.close();
			}
		}
	}

	public static void batchInTransaction(List<String> sqls, List<List<Object>> paramsList) throws SQLException {

		if (sqls == null || sqls.isEmpty()) {
			return;
		}

		boolean autoCommit = true;
		Connection conn = null;
		try {
			conn = dsUtil.getConnection();
			autoCommit = conn.getAutoCommit();
			// 关闭自动提交，即开启事务
			conn.setAutoCommit(false);
			long start = System.currentTimeMillis();
			for (int k = 0; k < 1000; k++) {
				for (int i = 0; i < sqls.size(); i++) {
					PreparedStatement stmt = conn.prepareStatement(sqls.get(i));
					for (int j = 0; j < paramsList.get(i).size(); j++) {
						stmt.setObject(j + 1, paramsList.get(i).get(j));
					}
					stmt.executeUpdate();
				}
			}
			// 提交事务
			conn.commit();
			System.out.println("used " + (System.currentTimeMillis() - start) + " ms.");
		} catch (SQLException e) {
			conn.rollback();
			throw e;
		} finally {
			if (conn != null) {
				conn.setAutoCommit(autoCommit);
				conn.close();
			}
		}
	}

	private static Connection getConnection() throws SQLException {
		return dsUtil.getConnection();
	}

	public static Map<String, Map<String, String>> getTableSchemas() {
		return tableSchemas;
	}
}
