/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import java.sql.*;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * DataSource with JDBC Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-05
 */

public class DataSourceJDBCExample {
    // 数据库配置
    private static final String URL = "jdbc:h2:~/test";
    private static final String DRIVER = "org.h2.Driver";
    private static final String USER = "sa";
    private static final String PASSWORD = "";
    private static final String TABLE_NAME = "users";
    private static final String CREATE_TABLE_SQL =
        "create table users ( id  int primary key, name varchar(20), homePage varchar(40) );";
    private static final String INSERT_USERS_SQL =
        "INSERT INTO users" + "  (id, name, homePage ) VALUES " + " (?, ?, ?);";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // 注册JDBC驱动
        Class.forName(DRIVER);

        // 初始化表和数据
        createTable();
        insertRecord();

        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DataSourceJDBCExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 创建DataFrame
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> df = sparkSession.read()
            // JDBC数据源
            .format("jdbc").option("url", URL).option("driver", DRIVER).option("user", USER)
            .option("password", PASSWORD).option("dbtable", TABLE_NAME).load();

        // 将DataFrame的内容显示
        df.show();

        // *** 上面等同于以下使用DataFrameReader的jdbc方法 ***

        // 将数据库配置信息封装到Properties对象里面
        Properties connectionProperties = new Properties();
        connectionProperties.put("driver", DRIVER);
        connectionProperties.put("user", USER);
        connectionProperties.put("password", PASSWORD);

        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> dfJDBC = sparkSession.read()
            // JDBC数据源
            .jdbc(URL, TABLE_NAME, connectionProperties);

        // 将DataFrame的内容显示
        dfJDBC.show();

        // *** 设置查询选项 ***

        /*
        // 以下是一个错误示例！dbtable、query两个选项不能同时设置，否则报异常
        Properties connectionQueryProperties = new Properties();
        connectionQueryProperties.put("driver", driver);
        connectionQueryProperties.put("user", user);
        connectionQueryProperties.put("password", password);
        connectionQueryProperties.put("query", "select name, email from users");
        
        Dataset<Row> dfQuery = sparkSession
        	// 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        	.read()
        	// JDBC数据源
        	.jdbc(url, dbtable, connectionQueryProperties);
        */

        // 创建DataFrame
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> dfQuery = sparkSession.read()
            // JDBC数据源
            .format("jdbc").option("url", URL).option("driver", DRIVER).option("user", USER)
            .option("password", PASSWORD)
            // 设置查询语句
            .option("query", "select name, email from users").load();

        // 将DataFrame的内容显示
        dfQuery.show();

        // *** 写入数据 ***
        // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
        dfQuery.write()
            // JDBC数据源
            .format("jdbc")
            // 如果第一次生成了，后续会追加
            .mode(SaveMode.Append).option("url", URL).option("driver", DRIVER).option("user", USER)
            .option("password", PASSWORD).option("dbtable", TABLE_NAME).save();

        // 等同于上述方式
        // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
        dfQuery.write()
            // 如果第一次生成了，后续会追加
            .mode(SaveMode.Append)
            // JDBC数据源
            .jdbc(URL, TABLE_NAME, connectionProperties);

        // 关闭SparkSession
        sparkSession.stop();

    }

    // 建表
    private static void createTable() throws SQLException {
        System.out.println(CREATE_TABLE_SQL);
        // 建立连接
        try (Connection connection = getConnection();
            // 创建Statement
            Statement statement = connection.createStatement();) {

            // 执行SQL
            statement.execute(CREATE_TABLE_SQL);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    // 插入数据
    private static void insertRecord() throws SQLException {
        System.out.println(INSERT_USERS_SQL);
        // 建立连接
        try (Connection connection = getConnection();
            // 创建PreparedStatement
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_USERS_SQL)) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "waylau");
            preparedStatement.setString(3, "https://waylau.com");
            System.out.println(preparedStatement);

            // 执行SQL
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 获取连接
    private static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

}