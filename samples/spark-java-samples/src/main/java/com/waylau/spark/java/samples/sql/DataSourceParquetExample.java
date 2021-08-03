/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * DataSource with Parquet Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-02
 */
public class DataSourceParquetExample {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder()
				.appName("DataSourceParquet`") // 设置应用名称
				.master("local") // 本地单线程运行
				.getOrCreate();

		// *** 使用默认指定数据源格式 ***
		// 创建DataFrame
		Dataset<Row> df = sparkSession.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.load("src/main/resources/users.parquet"); // 加载存储于Parquet格式的Dataset

		// 将DataFrame的内容显示
		df.show();

		// 打印schema
		df.printSchema();

		// 指定列名来查询相应列的数据
		df.select("name", "favorite_color").show();

		// 将DataFrame写入外部存储系统
		df.select("name", "favorite_color")
				.write() // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
				.mode(SaveMode.Overwrite) // 如果第一次生成了，后续会覆盖
				.save("target/outfile/users_name_favorite_color.parquet");// 将DataFrame的内容保存在指定路径下

		// *** 指定数据源格式 ***
		/*
		Dataset<Row> peopleDF = sparkSession
				.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.format("json") // 指定数据源格式为json
				.load("src/main/resources/people.json");

		peopleDF.select("name", "age")
				.write() // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
				.format("parquet") // 指定数据源格式为parquet
				.mode(SaveMode.Overwrite) // 如果第一次生成了，后续会覆盖
				.save("target/outfile/people_name_age.parquet"); // 将DataFrame的内容保存在指定路径下
		*/
		// 等同于以下简化方式
		Dataset<Row> peopleDF = sparkSession
				.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.json("src/main/resources/people.json"); // 指定数据源格式为json

		peopleDF.select("name", "age")
				.write() // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
				.mode(SaveMode.Overwrite) // 如果第一次生成了，后续会覆盖
				.parquet(
						"target/outfile/people_name_age.parquet"); // 指定数据源格式为parquet

		// 关闭SparkSession
		sparkSession.stop();

	}

}