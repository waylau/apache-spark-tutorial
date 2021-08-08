/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

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

	public static void main(String[] args) {
		// 数据库配置
		String url = "jdbc:mysql://10.113.186.250:8066/mrp";
		String driver = "com.mysql.cj.jdbc.Driver";
		String user = "你的账号";
		String password = "你的密码";
		String dbtable = "batch_t";

		SparkSession sparkSession = SparkSession.builder()
				.appName("DataSourceJDBC") // 设置应用名称
				.master("local") // 本地单线程运行
				.getOrCreate();

		// 创建DataFrame
		Dataset<Row> df = sparkSession
				.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.format("jdbc") // JDBC数据源
				.option("url", url)
				.option("driver", driver)
				.option("user", user)
				.option("password", password)
				.option("dbtable", dbtable) // 表名
				.load();

		// 将DataFrame的内容显示
		df.show();

		// *** 上面等同于以下使用DataFrameReader的jdbc方法 ***

		// 将数据库配置信息封装到Properties对象里面
		Properties connectionProperties = new Properties();
		connectionProperties.put("driver", driver);
		connectionProperties.put("user", user);
		connectionProperties.put("password", password);

		Dataset<Row> dfJDBC = sparkSession
				.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.jdbc(url, dbtable, connectionProperties); // JDBC数据源

		// 将DataFrame的内容显示
		dfJDBC.show();

		// *** 设置查询选项 ***

		/*
		// 以下是一个错误示例！dbtable、query两个选项不能同时设置，否则报异常
		Properties connectionQueryProperties = new Properties();
		connectionQueryProperties.put("driver", driver);
		connectionQueryProperties.put("user", user);
		connectionQueryProperties.put("password", password);
		connectionQueryProperties.put("query", "select plan_date, status from batch_t");

		Dataset<Row> dfQuery = sparkSession
			.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
			.jdbc(url, dbtable, connectionQueryProperties); // JDBC数据源
		*/

		// 创建DataFrame
		Dataset<Row> dfQuery = sparkSession
				.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.format("jdbc") // JDBC数据源
				.option("url", url)
				.option("driver", driver)
				.option("user", user)
				.option("password", password)
				.option("query",
						"select plan_date, status from batch_t") // 设置查询语句
				.load();

		// 将DataFrame的内容显示
		dfQuery.show();

		// *** 写入数据 ***
		dfQuery.write() // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
				.format("jdbc") // JDBC数据源
				.mode(SaveMode.Append) // 如果第一次生成了，后续会追加
				.option("url", url)
				.option("driver", driver)
				.option("user", user)
				.option("password", password)
				.option("dbtable", dbtable) // 表名
				.save();

		// 等同于上述方式
		dfQuery.write() // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
				.mode(SaveMode.Append) // 如果第一次生成了，后续会追加
				.jdbc(url, dbtable, connectionProperties); // JDBC数据源

		// 关闭SparkSession
		sparkSession.stop();

	}

}