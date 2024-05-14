/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

/**
 * SparkStreaming saveAsTextFiles Sample
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-05-14
 */

public class SparkStreamingSaveAsTextFilesSample {

    public static void main(String[] args)
            throws InterruptedException {
        // 配置
        SparkConf conf = new SparkConf()
                // 本地多线程运行。不能设置成单线程
                .setMaster("local[*]")
                // 设置应用名称
                .setAppName("SparkStreamingSaveAsTextFilesSample");

        // Streaming上下文，每隔10秒执行一次获取批量数据然后处理这些数据
        JavaStreamingContext javaStreamingContext =
                new JavaStreamingContext(conf, Durations.seconds(10));

        // 建立输入流，从这个输入流里获取数据，用socket建立，这里是连接到本地的9999
        JavaReceiverInputDStream<String> lines =
                javaStreamingContext.socketTextStream(
                        "localhost", 9999);

        // 从输入流获取的数据，进行切分
        JavaDStream<String> words =
                lines.flatMap(x -> Arrays
                        .asList(x.split(" ")).iterator());

        // 转为DStream
        DStream<String> dstream = words.dstream();

        // 保存到指定文件
        dstream.saveAsTextFiles("output/words-output", "txt");

        // 启动JavaStreamingContext
        javaStreamingContext.start();

        // 等待JavaStreamingContext被中断
        javaStreamingContext.awaitTermination();
    }

}