import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestConn {


	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static ResultSet res;

	public static void main(String[] args) throws Exception {
		Class.forName(driverName);// 加载驱动
		Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.170.133:10000/default", "hive", "123456");// 建立连接
		Statement stmt = conn.createStatement();// 声明
		res = stmt.executeQuery("select * from goods");// 用声明stmt对象下的executeQuery方法查询
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2)+ "\t" + res.getString(3));

		}
		res.close();
		stmt.close();
		conn.close();

	}

}
