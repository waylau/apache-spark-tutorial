/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * DataSource with Parquet Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-02
 */
public class DataSourceParquetExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DataSourceParquetExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // *** 使用默认指定数据源格式 ***
        // 创建DataFrame
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> df = sparkSession.read()
            // 加载存储于Parquet格式的Dataset
            .load("users.parquet");

        // 将DataFrame的内容显示
        df.show();

        // 打印schema
        df.printSchema();

        // 指定列名来查询相应列的数据
        df.select("name", "favorite_color").show();

        // 将DataFrame写入外部存储系统
        // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
        df.select("name", "favorite_color").write()
            // 如果第一次生成了，后续会覆盖
            .mode(SaveMode.Overwrite)
            // 将DataFrame的内容保存在指定路径下
            .save("output/users_name_favorite_color.parquet");

        // *** 指定数据源格式 ***

        /*// 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> peopleDF = sparkSession.read()
            // 指定数据源格式为json
            .format("json").load("people.json");
        
        // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
        peopleDF.select("name", "age").write()
            // 指定数据源格式为parquet
            .format("parquet")
            // 如果第一次生成了，后续会覆盖
            .mode(SaveMode.Overwrite)
            // 将DataFrame的内容保存在指定路径下
            .save("output/people_name_age.parquet");*/

        // 等同于以下简化方式
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> peopleDF = sparkSession.read()
            // 指定数据源格式为json
            .json("people.json");

        // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
        peopleDF.select("name", "age").write()
            // 如果第一次生成了，后续会覆盖
            .mode(SaveMode.Overwrite)
            // 指定数据源格式为parquet
            .parquet("output/people_name_age.parquet");

        // 关闭SparkSession
        sparkSession.stop();

    }

}