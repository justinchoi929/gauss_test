package top.justinchoi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcConn {
    public static void main(String[] args) throws SQLException {
        Connection conn =  getConnect();
        Statement statement = conn.createStatement();
//        statement.execute("create table test_table (id int,name varchar (10))");
//        statement.execute("insert into test_table (id,name) values (1,'lisi')");
//        PreparedStatement preparedStatement=conn.prepareStatement("select * from test_table;");
//        ResultSet resultSet=preparedStatement.executeQuery();
//        while (resultSet.next()){
//            System.out.println(resultSet.getObject("id")+"  "+
//                    resultSet.getObject("name"));
//        }
//        conn.close();
        PreparedStatement preparedStatement=conn.prepareStatement("select * from test_table where id=?;");
        preparedStatement.setObject(1,2);
        ResultSet resultSet=preparedStatement.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getObject("id")+"  "+
                    resultSet.getObject("name"));
        }
        conn.close();
    }
    public static Connection getConnect() {
        String driver = "org.postgresql.Driver";
        String sourceURL = "jdbc:postgresql://117.72.69.73:7654/test?user=justin&password=Test@123";
        Properties info = new Properties();
        Connection conn = null;
        try {
            Class.forName(driver);
        } catch (Exception var9) {
            var9.printStackTrace();
            return null;
        }
        try {
            conn = DriverManager.getConnection(sourceURL);
            System.out.println("连接成功！");
            return conn;
        } catch (Exception var8) {
            var8.printStackTrace();
            return null;
        }
    }
}

