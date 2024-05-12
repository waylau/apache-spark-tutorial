/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataFrame Basic Example
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-02
 */

public class DataFrameBasicExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DataFrameBasicExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 创建DataFrame
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> df = sparkSession.read()
            // 加载存储JSON对象的Dataset
            .json("people.json");

        // 将DataFrame的内容显示
        df.show();

        // 打印schema
        df.printSchema();

        // 指定列名来查询相应列的数据
        df.select("name").show();

        // 指定多个列名
        df.select("name", "age").show();

        // 上述等同于col函数
        df.select(col("name"), col("age")).show();

        // plus递增1
        df.select(col("name"), col("age").plus(1)).show();

        // 过滤大于21
        df.filter(col("age").gt(21)).show();

        // 分组并统计各组个数
        df.groupBy("age").count().show();

        // 关闭SparkSession
        sparkSession.stop();

    }

}