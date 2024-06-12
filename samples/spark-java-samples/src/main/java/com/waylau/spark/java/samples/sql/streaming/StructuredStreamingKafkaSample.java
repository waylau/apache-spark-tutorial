/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql.streaming;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * StructuredStreaming Kafka Sample
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-06-11
 */
public class StructuredStreamingKafkaSample {
    /**
     * 定义主题
     */
    private static String TOPIC = "test_topic";

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("StructuredStreamingKafkaSample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 创建一个流式DataFrame，该流式DataFrame表示从Kafka接收数据
        Dataset<Row> df = sparkSession
            // 返回一个DataStreamReader，可用于将流数据作为DataFrame读取
            .readStream()
            // 设置数据源格式
            .format("kafka")
            // 服务器地址
            .option("kafka.bootstrap.servers", "192.168.1.78:9094")
            // 主题
            .option("subscribe", TOPIC).load();

        // 解析 Kafka 的 value 列
        df = df.selectExpr("CAST(value AS STRING)");

        // 开始运行查询，将运行计数打印到控制台
        StreamingQuery query = df.writeStream()
            // append模式
            .outputMode("append")
            // 输出到控制台
            .format("console").start();

        // 等待StreamingQuery被中断
        query.awaitTermination();
    }

}