/*
* Copyright (c) waylau.com, 2021. All rights reserved.
*/

package com.waylau.spark.java.samples.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * StructuredStreaming Window Sample
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-11
 */
public class StructuredStreamingWindowSample {

	public static void main(String[] args)
			throws TimeoutException,
			StreamingQueryException {

		SparkSession sparkSession = SparkSession.builder()
				.appName("StructuredStreamingWindow") // 设置应用名称
				.master("local") // 本地单线程运行
				.getOrCreate();

		// 创建一个流式DataFrame，该流式DataFrame表示从侦听localhost:9999的服务器接收的文本数据
		Dataset<Row> lines = sparkSession
				.readStream() // 返回一个DataStreamReader，可用于将流数据作为DataFrame读取
				.format("socket") // 设置数据源格式
				.option("host", "localhost") // 服务器地址
				.option("port", 9999) // 端口
				.option("includeTimestamp", true) // 输出内容包括时间戳
				.load();

		// 将lines拆成words，保留时间戳
		Dataset<Row> words = lines
				.as(Encoders.tuple(Encoders.STRING(),
						Encoders.TIMESTAMP()))
				.flatMap(
						(FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
							List<Tuple2<String, Timestamp>> result = new ArrayList<>();

							for (String word : t._1
									.split(" ")) {
								result.add(new Tuple2<>(
										word, t._2));
							}

							return result.iterator();

						},
						Encoders.tuple(Encoders.STRING(),
								Encoders.TIMESTAMP()))

				.toDF("word", "timestamp");

		// 统计词频
		// 按窗口和单词对数据进行分组，并计算每组的计数
		Dataset<Row> wordCounts = words
				.groupBy(
						functions.window(
								words.col("timestamp"), //
								"10 seconds", // 窗口长度
								"5 seconds"), // 滑动间隔
						words.col("word"))
				.count().orderBy("window");

		// 开始运行查询，将运行计数打印到控制台
		StreamingQuery query = wordCounts
				.writeStream()
				.outputMode("complete") // 必须是complete模式
				.format("console")
				.option("truncate", "false") // 不清理
				.start();

		// 等待StreamingQuery被中断
		query.awaitTermination();

	}
}