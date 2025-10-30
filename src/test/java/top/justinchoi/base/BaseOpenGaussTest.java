package top.justinchoi.base;

import top.justinchoi.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * 基础测试类，提供openGauss数据库的基本功能测试
 * 使用TestUtil工具类进行数据库连接和操作
 */
public class BaseOpenGaussTest {
    protected Connection connection;
    protected static final String TEST_TABLE = "test_basic_table";
    
    /**
     * 测试前初始化，建立数据库连接
     */
    @Before
    public void setUp() throws Exception {
        System.out.println("开始初始化测试环境...");
        connection = TestUtil.getConnection();
        assertNotNull("数据库连接失败", connection);
        assertTrue("数据库连接未打开", !connection.isClosed());
        
        // 创建测试表
        String createTableSql = "CREATE TABLE " + TEST_TABLE + " (" +
                "id SERIAL PRIMARY KEY, " +
                "name VARCHAR(100) NOT NULL, " +
                "age INT, " +
                "score DECIMAL(10,2), " +
                "is_active BOOLEAN" +
                ")";
        TestUtil.createTable(connection, TEST_TABLE, createTableSql);
        
        // 插入测试数据
        insertTestData();
        System.out.println("测试环境初始化完成");
    }
    
    /**
     * 插入测试数据
     */
    private void insertTestData() throws SQLException {
        String insertSql = "INSERT INTO " + TEST_TABLE + " (name, age, score, is_active) VALUES (?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setString(1, "张三");
            pstmt.setInt(2, 25);
            pstmt.setBigDecimal(3, new java.math.BigDecimal(95.5));
            pstmt.setBoolean(4, true);
            pstmt.executeUpdate();
            
            pstmt.setString(1, "李四");
            pstmt.setInt(2, 30);
            pstmt.setBigDecimal(3, new java.math.BigDecimal(88.0));
            pstmt.setBoolean(4, false);
            pstmt.executeUpdate();
            
            pstmt.setString(1, "王五");
            pstmt.setInt(2, 22);
            pstmt.setBigDecimal(3, new java.math.BigDecimal(92.75));
            pstmt.setBoolean(4, true);
            pstmt.executeUpdate();
        }
    }
    
    /**
     * 测试后清理，关闭连接并删除测试表
     */
    @After
    public void tearDown() throws Exception {
        System.out.println("开始清理测试环境...");
        if (connection != null && !connection.isClosed()) {
            // 删除测试表
            TestUtil.dropTable(connection, TEST_TABLE);
            // 关闭连接
            connection.close();
        }
        System.out.println("测试环境清理完成");
    }
    
    /**
     * 测试基本查询功能
     */
    @Test
    public void testBasicQuery() throws SQLException {
        System.out.println("执行基本查询测试...");
        String querySql = "SELECT id, name, age, score, is_active FROM " + TEST_TABLE + " ORDER BY id";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(querySql)) {
            
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                int id = rs.getInt("id");
                String name = rs.getString("name");
                int age = rs.getInt("age");
                double score = rs.getDouble("score");
                boolean isActive = rs.getBoolean("is_active");
                
                System.out.println("查询结果: id=" + id + ", name=" + name + ", age=" + age + ", score=" + score + ", is_active=" + isActive);
                assertNotNull("ID不能为空", id);
                assertNotNull("姓名不能为空", name);
            }
            
            assertEquals("记录数应为3", 3, rowCount);
        }
    }
    
    /**
     * 测试更新功能
     */
    @Test
    public void testUpdate() throws SQLException {
        System.out.println("执行更新测试...");
        String updateSql = "UPDATE " + TEST_TABLE + " SET age = ?, score = ? WHERE name = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(updateSql)) {
            pstmt.setInt(1, 26);
            pstmt.setBigDecimal(2, new java.math.BigDecimal(97.5));
            pstmt.setString(3, "张三");
            
            int affectedRows = pstmt.executeUpdate();
            assertEquals("应该更新1条记录", 1, affectedRows);
            
            // 验证更新结果
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT age, score FROM " + TEST_TABLE + " WHERE name = '张三'");) {
                if (rs.next()) {
                    assertEquals("年龄应该更新为26", 26, rs.getInt("age"));
                    assertEquals("分数应该更新为97.5", 97.5, rs.getDouble("score"), 0.01);
                } else {
                    fail("未找到更新的记录");
                }
            }
        }
    }
    
    /**
     * 测试删除功能
     */
    @Test
    public void testDelete() throws SQLException {
        System.out.println("执行删除测试...");
        String deleteSql = "DELETE FROM " + TEST_TABLE + " WHERE is_active = false";
        
        try (Statement stmt = connection.createStatement()) {
            int affectedRows = stmt.executeUpdate(deleteSql);
            assertTrue("至少应该删除1条记录", affectedRows >= 1);
            
            // 验证删除结果
            int rowCount = TestUtil.getRowCount(connection, TEST_TABLE);
            assertTrue("删除后记录数应小于3", rowCount < 3);
        }
    }
    
    /**
     * 测试事务处理
     */
    @Test
    public void testTransaction() throws SQLException {
        System.out.println("执行事务测试...");

        try {
            connection.setAutoCommit(false);

            // 插入记录（预期成功）
            String insertSql = "INSERT INTO " + TEST_TABLE + " (name, age, score, is_active) VALUES ('赵六', 28, 90.0, true)";
            try (Statement stmt = connection.createStatement()) {
                int insertRows = stmt.executeUpdate(insertSql);
                assertEquals("插入记录失败", 1, insertRows); // 先断言插入成功
            }

            // 验证插入后记录数（此时事务未提交，记录数应为4）
            int rowCount = TestUtil.getRowCount(connection, TEST_TABLE);
            assertEquals("事务中记录数应为4", 4, rowCount);

            // 提交事务（若需验证提交，执行commit；若验证回滚，执行rollback）
            connection.commit(); // 改为commit，确保记录持久化
            // connection.rollback(); // 回滚会撤销插入，导致记录数回到3

            // 再次验证记录数（提交后应为4）
            rowCount = TestUtil.getRowCount(connection, TEST_TABLE);
            assertEquals("提交后记录数应为4", 4, rowCount);

        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    /**
     * 测试异常处理 - 插入重复主键
     */
    @Test(expected = SQLException.class)
    public void testErrorHandling() throws SQLException {
        System.out.println("执行异常处理测试...");
        
        // 尝试插入重复ID（SERIAL主键）
        String errorSql = "INSERT INTO " + TEST_TABLE + " (id, name) VALUES (1, '重复ID')";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(errorSql);
        }
    }
    
    /**
     * 主方法，支持直接运行测试
     */
    public static void main(String[] args) {
        BaseOpenGaussTest test = new BaseOpenGaussTest();
        try {
            test.setUp();
            
            // 运行所有测试
            test.testBasicQuery();
            test.testUpdate();
            test.testDelete();
            test.testTransaction();
            
            try {
                test.testErrorHandling();
                System.out.println("注意：testErrorHandling应抛出SQLException但未抛出");
            } catch (SQLException e) {
                System.out.println("testErrorHandling正常抛出SQLException：" + e.getMessage());
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                test.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}