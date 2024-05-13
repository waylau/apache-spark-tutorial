/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataSource with Binary File Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-05
 */

public class DataSourceBinaryFileExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                // 设置应用名称
                .appName("DataSourceBinaryFileExample")
                // 本地单线程运行
                .master("local").getOrCreate();

        // 创建DataFrame
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> df = sparkSession.read()
                // 二进制文件数据源
                .format("binaryFile")
                // 设置过滤策略
                .option("pathGlobFilter", "*.png")
                // 加载存储于二进制文件格式的Dataset
                .load("./");

        // 将DataFrame的内容显示
        df.show();

        // 关闭SparkSession
        sparkSession.stop();
    }

}