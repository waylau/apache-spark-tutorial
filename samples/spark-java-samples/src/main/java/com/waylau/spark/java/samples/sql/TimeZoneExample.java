/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Time Zone Example.
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-09-08
 */
public class TimeZoneExample {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder()
				.appName("TimeZone") // 设置应用名称
				.master("local") // 本地单线程运行
				.config("spark.sql.session.timeZone", "UTC+8") // 设置为UTC
				.getOrCreate();

		// 创建Dataset
		Dataset<Row> df = sparkSession
				.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.json("src/main/resources/datedPeople.json"); // 加载存储JSON对象的Dataset

		// 将DataFrame的内容显示
		df.show(20, 30);

		// 打印schema
		df.printSchema();
		df.select(col("name"),
				to_timestamp(col("birthdate")))
				.show(20, 30);

		// 关闭SparkSession
		sparkSession.stop();

	}

}