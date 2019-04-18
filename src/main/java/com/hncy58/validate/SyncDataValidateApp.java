package com.hncy58.validate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hncy58.ds.DataSourceUtil;
import com.hncy58.util.Utils;

/**
 * 同步数据校验APP
 * 
 * @author tdz
 * @company hncy58 长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年11月12日 下午4:42:54
 */
public class SyncDataValidateApp {

	private static String url = "jdbc:mysql://162.16.4.9:3306?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
	private static String driverClass = "com.mysql.jdbc.Driver";
	private static String username = "root";
	private static String password = "cjml@1234";
	private static String querySql = "select * from bigdata.sync_data_validate_cfg where status = 1";

	public static void main(String[] args) throws Exception {

		DataSourceUtil ds = new DataSourceUtil();
		ds.initDataSource(url, username, password, driverClass, 2, 2, 1, 50000);

		List<Map<String, Object>> syncTableCfgs = ds.query(querySql, new Object[] {});
		ds.shutdownDataSource();

		Map<String, Map<Integer, List<Map<String, Object>>>> ret = new HashMap<>();

		syncTableCfgs.forEach(syncTableMap -> {

			System.out.println("start to validate --------------------" + syncTableMap);

			int id = Utils.toInt(syncTableMap.get("id"));
			String validate_grp = Utils.toString(syncTableMap.get("validate_grp"));
			int source_type = Utils.toInt(syncTableMap.get("source_type"));
			int fail_process_mode = Utils.toInt(syncTableMap.get("fail_process_mode"));

			String username = Utils.toString(syncTableMap.get("username"));
			String pwd = Utils.toString(syncTableMap.get("pwd"));
			String db_name = Utils.toString(syncTableMap.get("db_name"));
			String tbl_name = Utils.toString(syncTableMap.get("tbl_name"));
			String validate_exp = Utils.toString(syncTableMap.get("validate_exp"));
			String where_clause = Utils.toString(syncTableMap.get("where_clause"));
			String con_url = Utils.toString(syncTableMap.get("con_url"));
			String driver_class = Utils.toString(syncTableMap.get("driver_class"));

			Connection conn = null;
			ResultSet rs = null;
			PreparedStatement ps = null;

			try {
				Class.forName(driver_class);
				conn = DriverManager.getConnection(con_url, username, pwd);
				StringBuffer sql = new StringBuffer("");
				sql.append("select ").append(validate_exp).append(" from ").append(db_name).append(".").append(tbl_name)
						.append(" where 1=1 ").append(Utils.isEmpty(where_clause) ? "" : "and " + where_clause);

				ps = conn.prepareStatement(sql.toString());
				rs = ps.executeQuery();

				List<Map<String, Object>> srcData = mapResultSet(rs);
//				System.out.println("source_type:" + source_type + " validate " + db_name + "." + tbl_name
//						+ " result -------------------" + srcData);

				srcData.forEach(dataMap -> {

					Map<Integer, List<Map<String, Object>>> retTuple = null;

					if (ret.containsKey(validate_grp)) {
						retTuple = ret.get(validate_grp);
					} else {
						retTuple = new HashMap<>();
						ret.put(validate_grp, retTuple);
					}

					List<Map<String, Object>> dataArr = null;
					if (retTuple.containsKey(source_type)) {
						dataArr = retTuple.get(source_type);
					} else {
						dataArr = new ArrayList<>();
						retTuple.put(source_type, dataArr);
					}
					
					// 将原始的配置ID写入结果集中
					dataMap.put("cfg_id", id);
					dataArr.add(dataMap);
				});

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (rs != null)
						rs.close();
					if (ps != null)
						ps.close();
					if (conn != null)
						conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});

		System.out.println("========================");
		System.out.println(ret);
		
	}

	private static List<Map<String, Object>> mapResultSet(ResultSet rs) throws SQLException {

		DecimalFormat df = new DecimalFormat("#0.0000");
		List<Map<String, Object>> data = new ArrayList<>();
		ResultSetMetaData meta = rs.getMetaData();

		List<String> names = new ArrayList<>();

		for (int i = 0; i < meta.getColumnCount(); i++) {
			names.add(meta.getColumnLabel(i + 1));
		}

		while (rs.next()) {
			Map<String, Object> map = new HashMap<>();
			int type = Types.OTHER;
			int cnt = 1;
			for (String name : names) {
				type = meta.getColumnType(cnt);
				if(type == 3 || type == 8 || type == 6) {
					map.put(name, df.format(rs.getDouble(name)));
				} else {
					map.put(name, rs.getObject(name));
				}
				cnt ++;
			}
			data.add(map);
		}

		return data;
	}
}
