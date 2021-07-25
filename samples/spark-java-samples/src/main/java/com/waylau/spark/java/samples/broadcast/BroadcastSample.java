/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.broadcast;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * Broadcast Sample。
 * 
 * @since 1.0.0 2021年7月25日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
public class BroadcastSample {

	public static void main(String[] args) {
		// 要构建一个包含有Spark关应用程序信息的SparkConf对象
		SparkConf conf = new SparkConf()
				.setAppName("CollectionAccumulator")// 设置应用名称
				.setMaster("local[4]"); // 本地4核运行

		// 创建一个JavaSparkContext对象，它告诉Spark如何访问集群
		JavaSparkContext sparkContext = new JavaSparkContext(
				conf);

		// 创建RDD
		List<Double> data = Arrays.asList(1.1D, 2.2D, 3.3D,
				4.4D, 5.5D, 6.5D);
		JavaRDD<Double> rdd = sparkContext.parallelize(data,
				5);

		// 创建Broadcast
		List<Integer> broadcastData = Arrays.asList(1, 2, 3,
				4, 5);

		final Broadcast<List<Integer>> broadcast = sparkContext
				.broadcast(broadcastData);

		// 获取Broadcast值
		rdd.foreach(x -> {
			System.out.println("broadcast id: "
					+ broadcast.id() + ", value: "
					+ broadcast.value());
		});

		// 关闭JavaSparkContext
		sparkContext.close();
	}

}
