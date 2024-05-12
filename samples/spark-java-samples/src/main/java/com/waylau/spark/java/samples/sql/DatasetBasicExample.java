package com.waylau.spark.java.samples.sql;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.waylau.spark.java.samples.common.Person;

/**
 * Dataset Basic Example.
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-02
 */
public class DatasetBasicExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DatasetBasicExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 创建Java Bean的编码器
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        // 创建Dataset
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Person> personDataset = sparkSession.read()
            // 加载存储JSON对象的Dataset
            .json("people.json")
            // 指定编码器，将DataFrame转为Dataset
            .as(personEncoder);

        // 将Dataset的内容显示
        personDataset.show();

        // 打印schema
        personDataset.printSchema();

        // 指定列名来查询相应列的数据
        personDataset.select("name").show();

        // 指定多个列名
        personDataset.select("name", "age").show();

        // 上述等同于col函数
        personDataset.select(col("name"), col("age")).show();

        // plus递增1
        personDataset.select(col("name"), col("age").plus(1)).show();

        // 过滤大于21
        personDataset.filter(col("age").gt(21)).show();

        // 分组并统计各组个数
        personDataset.groupBy("age").count().show();

        // 关闭SparkSession
        sparkSession.stop();

    }

}