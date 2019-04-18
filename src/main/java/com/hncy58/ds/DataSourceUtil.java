package com.hncy58.ds;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//@Deprecated
public class DataSourceUtil {

	private static final Logger log = LoggerFactory.getLogger(DSPoolUtil.class);

	// 持有一个静态的数据库连接池对象
	private DataSource dataSource = null;
	private boolean inited = false;

	public DataSourceUtil() {
		super();
	}

	public DataSourceUtil(String connectURI, String username, String password, String driverClass, int initialSize,
			int maxTotal, int maxIdle, int maxWaitMillis) {
		super();
		initDataSource(connectURI, username, password, driverClass, initialSize, maxTotal, maxIdle, maxWaitMillis);
	}

	// 使用DBCP提供的BasicDataSource实现DataSource接口
	public void initDataSource(String connectURI, String username, String password, String driverClass, int initialSize,
			int maxTotal, int maxIdle, int maxWaitMillis) {
		synchronized (this) {
			BasicDataSource ds = new BasicDataSource();
			ds.setDriverClassName(driverClass);
			ds.setUsername(username);
			ds.setPassword(password);
			ds.setUrl(connectURI);
			ds.setInitialSize(initialSize);
			ds.setMaxTotal(maxTotal);
			ds.setMaxIdle(maxIdle);
			ds.setMaxWaitMillis(maxWaitMillis);
			dataSource = ds;
			inited = true;
		}
	}

	// 获得连接对象
	public Connection getConnection() throws SQLException {

		if (!inited) {
			throw new SQLException("数据库未初始化！");
		}

		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		return conn;
	}

	public void release(Connection conn, Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				// 关闭存储查询结果的ResultSet对象
				rs.close();
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
			rs = null;
		}
		if (st != null) {
			try {
				st.close();
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
		if (conn != null) {
			try {
				// 将Connection连接对象还给数据库连接池
				conn.close();
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
	}

	public void shutdownDataSource() throws SQLException {
		BasicDataSource bds = (BasicDataSource) dataSource;
		bds.close();
	}

	public int[] batchUpdate(String sql, List<List<Object>> paramsList) throws SQLException {

		Connection con = getConnection();
		PreparedStatement ps = con.prepareStatement(sql);
		con.setAutoCommit(false);

		for (List<Object> params : paramsList) {
			if (params != null) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}
			ps.addBatch();
		}

		int[] ret = null;
		try {
			ret = ps.executeBatch();
			con.commit();
		} catch (Exception e) {
			con.rollback();
			throw e;
		} finally {
			release(con, ps, null);
		}

		return ret;
	}

	public int update(String sql, Object... params) throws SQLException {

		Connection con = getConnection();
		PreparedStatement ps = con.prepareStatement(sql);
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
		}

		int ret = 0;

		try {
			ret = ps.executeUpdate();
		} finally {
			release(con, ps, null);
		}

		return ret;
	}

	public List<Map<String, Object>> query(String sql, Object... params) throws SQLException {

		ResultSet rs = null;
		Connection con = null;
		PreparedStatement ps = null;

		List<Map<String, Object>> data = new ArrayList<>();
		try {
			con = getConnection();
			ps = con.prepareStatement(sql);
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					ps.setObject(i + 1, params[i]);
				}
			}

			rs = ps.executeQuery();

			ResultSetMetaData meta = rs.getMetaData();

			List<String> names = new ArrayList<>();

			for (int i = 0; i < meta.getColumnCount(); i++) {
				names.add(meta.getColumnLabel(i + 1));
			}

			while (rs.next()) {
				Map<String, Object> map = new HashMap<>();
				for (String name : names) {
					map.put(name, rs.getObject(name));
				}
				data.add(map);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			release(con, ps, rs);
		}

		return data;
	}

}
