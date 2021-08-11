/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * StructuredStreaming Socket Sample
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-11
 */
public class StructuredStreamingSocketSample {

	public static void main(String[] args)
			throws TimeoutException,
			StreamingQueryException {
		SparkSession sparkSession = SparkSession.builder()
				.appName("StructuredStreamingSocket") // 设置应用名称
				.master("local") // 本地单线程运行
				.getOrCreate();

		// 创建一个流式DataFrame，该流式DataFrame表示从侦听localhost:9999的服务器接收的文本数据
		Dataset<Row> lines = sparkSession
				.readStream() // 返回一个DataStreamReader，可用于将流数据作为DataFrame读取
				.format("socket") // 设置数据源格式
				.option("host", "localhost") // 服务器地址
				.option("port", 9999) // 端口
				.load();

		// 将lines拆成words
		Dataset<String> words = lines.as(Encoders.STRING())
				.flatMap((FlatMapFunction<String, String>) x
				-> Arrays.asList(x.split(" ")).iterator(),
						Encoders.STRING());

		// 统计词频
		Dataset<Row> wordCounts = words.groupBy("value")
				.count();

		// 开始运行查询，将运行计数打印到控制台
		StreamingQuery query = wordCounts.writeStream()
				.outputMode("complete")
				.format("console")
				.start();

		// 等待StreamingQuery被中断
		query.awaitTermination();
	}

}