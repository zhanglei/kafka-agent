package com.hncy58.kafka;

import java.sql.SQLException;

import com.hncy58.ds.DSPoolUtil;

public class TestUpdateDB {

	static final String[] sqls = new String[] {
			"update riskcontrol.inf_customer_credit set MODIFY_DATE = now() where ACTIVED_DATE BETWEEN '2018-06-14 15:27:27.969000' and '2018-06-18 15:27:27.969000'",
			"update riskcontrol.inf_customer set MODIFY_DATE = now() where MOBILE_NO BETWEEN '15911018140' and '15920098140'",
			"update isop.customer set modifyDate = now() where Cust_Id BETWEEN 21000000010 and 21000025502",
			"update payment.dict_card_bin set CARD_NAME = now()",
			"update wechat.wechatmember set lastupdate = now() where subscribe_time < 1528888002000" };

	public static void main(String[] args) {

		while (true) {
			for (String sql : sqls) {
				try {
					long start = System.currentTimeMillis();
					int rows = DSPoolUtil.update(sql, new Object[] {});
					System.out.println("execute sql:" + sql + "\n\tupdate rows -> " + rows + ", used "
							+ (System.currentTimeMillis() - start) + " ms.");
				} catch (SQLException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			try {
				Thread.sleep(1 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
