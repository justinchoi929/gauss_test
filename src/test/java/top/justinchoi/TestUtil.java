package top.justinchoi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * 数据库测试工具类，提供连接管理和常用操作方法
 */
public class TestUtil {
    // 配置常量
    public static final String SERVER_HOST_PORT_PROP = "opengauss.jdbc.server.hostPort";
    public static final String DATABASE_PROP = "opengauss.jdbc.server.database";
    public static final String USER_PROP = "opengauss.jdbc.server.user";
    public static final String PASSWORD_PROP = "opengauss.jdbc.server.password";
    public static final String URL_PROP = "opengauss.jdbc.server.url";
    
    // 默认连接配置
    private static final String DEFAULT_DRIVER = "org.postgresql.Driver";
    private static final String DEFAULT_URL = "jdbc:postgresql://117.72.69.73:7654/test?user=justin&password=Test@123";
    
    private static String applicationName = "openGauss-JDBC-Test";
    
    /**
     * 获取数据库连接
     */
    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(DEFAULT_DRIVER);
        return DriverManager.getConnection(DEFAULT_URL);
    }
    
    /**
     * 获取数据库连接
     */
    public static Connection getConnection(String url, String user, String password) throws SQLException, ClassNotFoundException {
        Class.forName(DEFAULT_DRIVER);
        return DriverManager.getConnection(url, user, password);
    }
    
    /**
     * 关闭资源
     */
    public static void closeResources(Connection conn, Statement stmt, ResultSet rs) {
        try {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 创建表
     */
    public static void createTable(Connection conn, String tableName, String createSql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // 先尝试删除表
            stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            // 创建新表
            stmt.executeUpdate(createSql);
            System.out.println("表 " + tableName + " 创建成功");
        }
    }
    
    /**
     * 删除表
     */
    public static void dropTable(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            System.out.println("表 " + tableName + " 删除成功");
        }
    }
    
    /**
     * 清理表数据
     */
    public static void clearTable(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DELETE FROM " + tableName);
            System.out.println("表 " + tableName + " 数据清理成功");
        }
    }
    
    /**
     * 获取表记录数
     */
    public static int getRowCount(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
    
    /**
     * 构建URL
     */
    public static String getURL(String hostPort, String database, String user, String password) {
        return "jdbc:postgresql://" + hostPort + "/" + database + "?user=" + user + "&password=" + password;
    }
    
    /**
     * 构建URL
     */
    public static String getURL(String hostPort, String database, Properties info) {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://")
           .append(hostPort)
           .append("/")
           .append(database);
           
        if (info != null && !info.isEmpty()) {
            url.append("?");
            boolean first = true;
            for (String key : info.stringPropertyNames()) {
                if (!first) {
                    url.append("&");
                }
                url.append(key).append("=").append(info.getProperty(key));
                first = false;
            }
        }
        return url.toString();
    }
}