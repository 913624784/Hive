

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveAPITest {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static Connection conn;
	private static Statement stmt;
	private static ResultSet res;
	private static String sql = "";

	public static void main(String[] args) throws Exception {
		Class.forName(driverName);
		conn = DriverManager.getConnection("jdbc:hive2://192.168.170.133:10000/default", "user", "123456");
		stmt = conn.createStatement();
		// 创建的表名
		String tableName = "tb";
		/** 第一步：存在就先删除 **/
		sql = "drop table " + tableName;
		stmt.executeUpdate(sql);

		/** 第二步：如果表不存在就创建表tableName **/
		sql = "create table " + tableName + " (id int,name string,age int) ";
		sql += " row format delimited fields terminated by '\t'";
		stmt.executeUpdate(sql);

		// 执行“show tables”操作
		sql = "show tables '" + tableName + "'";
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行“show tables”运行结果：");
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// 执行“describe table”操作
		sql = "describe " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行“describe table”运行结果：");
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// 执行“load data into table”操作
		String filepath = "/home/user/h.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running:" + sql);
		stmt.executeUpdate(sql);

		// 执行“select * query”操作
		sql = "select * from " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行“select * query”运行结果：");
		while (res.next()) {
			System.out.println(res.getInt(1) + "\t" + res.getString(2) + "\t"
					+ res.getInt(3));
		}
		// 执行“regular hive query”操作
		/*sql = "select count(1) from " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行“regular hive query”运行结果：");
		while (res.next()) {
			System.out.println(res.getString(1));
		}*/
		res.close();
		stmt.close();
		conn.close();
	}
}
