/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataSource with Avro Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-05-27
 */
public class DataSourceAvroExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DataSourceAvroExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 创建DataFrame
        // 返回一个DataFrameReader，可用于读取文件
        Dataset<Row> df = sparkSession.read()
            // 文件格式Avro
            .format("avro")
            // 从指定路径读取文件
            .load("users.avro");

        // 写入文件
        df
            // 指定字段
            .select("name", "favorite_color")
            // 输出
            .write()
            // 文件格式Avro
            .format("avro")
            // 指定保存文件的路径
            .save("output/users.avro");

        // 将DataFrame的内容显示
        df.show();

        // 打印schema
        df.printSchema();

        // 关闭SparkSession
        sparkSession.stop();
    }

}