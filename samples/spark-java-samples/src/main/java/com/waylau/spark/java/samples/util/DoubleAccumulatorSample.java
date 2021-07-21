/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.util;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;

/**
 * DoubleAccumulator Sample
 * 
 * @since 1.0.0 2021年7月21日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
public class DoubleAccumulatorSample {

	public static void main(String[] args) {
		// 要构建一个包含有Spark关应用程序信息的SparkConf对象
		SparkConf conf = new SparkConf()
				.setAppName("DoubleAccumulator")// 设置应用名称
				.setMaster("local[4]"); // 本地4核运行

		// 创建一个JavaSparkContext对象，它告诉Spark如何访问群集
		JavaSparkContext sparkContext = new JavaSparkContext(
				conf);

		List<Double> data = Arrays.asList(1.1D, 2.2D, 3.3D,
				4.4D, 5.5D);

		// 创建一个可以并行操作的分布式数据集
		JavaRDD<Double> rdd = sparkContext
				.parallelize(data);

		// 来创建双精度浮点数累加器用于累计Double值
		DoubleAccumulator counter = sparkContext.sc()
				.doubleAccumulator();

		// 累计Double值
		rdd.foreach(x -> counter.add(x));

		// 读取累加器的结果
		System.out.println(
				"Counter value: " + counter.value());
		System.out.println("Counter avg: " + counter.avg());
		System.out.println(
				"Counter count: " + counter.count());
		System.out.println("Counter sum: " + counter.sum()); // 等同于value()

		// 关闭JavaSparkContext
		sparkContext.close();
	}

}
