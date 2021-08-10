/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.streaming;

import org.apache.spark.SparkConf;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;

import scala.Tuple2;

import java.util.Arrays;

/**
 * SparkStreaming Wimdow Sample
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-09
 */

public class SparkStreamingWimdowSample {

	public static void main(String[] args)
			throws InterruptedException {
		// 配置
		SparkConf conf = new SparkConf()
				.setMaster("local[*]") // 本地多线程运行。不能设置成单线程
				.setAppName("SparkStreamingWimdow");// 设置应用名称

		// Streaming上下文，每隔10秒执行一次获取批量数据然后处理这些数据
		JavaStreamingContext javaStreamingContext =

				new JavaStreamingContext(conf,
						Durations.seconds(10));

		// 建立输入流，从这个输入流里获取数据，用socket建立，这里是连接到本地的9999
		JavaReceiverInputDStream<String> lines =
				javaStreamingContext.socketTextStream(
						"localhost", 9999);

		// 从输入流获取的数据，进行切分
		JavaDStream<String> words =
				lines.flatMap(x -> Arrays
						.asList(x.split(" ")).iterator());

		// 统计词频
		JavaPairDStream<String, Integer> pairs =
				words.mapToPair(s -> new Tuple2<>(s, 1));

		// 每10秒来统计最后30秒数据
		JavaPairDStream<String, Integer> wordCounts = pairs
				.reduceByKeyAndWindow((i1, i2) -> i1 + i2,
						Durations.seconds(30), // 窗口长度
						Durations.seconds(10)); // 滑动间隔
		// 输出计数
		wordCounts.print();

		// 启动JavaStreamingContext
		javaStreamingContext.start();

		// 等待JavaStreamingContext被中断
		javaStreamingContext.awaitTermination();
	}

}