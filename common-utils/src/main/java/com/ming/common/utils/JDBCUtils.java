package com.ming.common.utils;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import javax.sql.DataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class JDBCUtils {
    // 数据库连接地址
    private static String URL;
    // 数据库用户名
    private static String USERNAME;
    // 数据库密码
    private static String PASSWORD;
    // 数据库驱动名
    private static String DRIVER;

    // 构造函数私有化
    private JDBCUtils() {
    }

    static {
        try {
            // 加载配置文件中的数据库连接信息
            InputStream inStream = JDBCUtils.class.getClassLoader()
                    .getResourceAsStream("jdbc.properties");
            Properties prop = new Properties();
            prop.load(inStream);
            URL = prop.getProperty("jdbc.url");
            USERNAME = prop.getProperty("jdbc.username");
            PASSWORD = prop.getProperty("jdbc.password");
            DRIVER = prop.getProperty("jdbc.driver");
            // 使用静态代码块加载数据库驱动
            Class.forName(DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭资源
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
