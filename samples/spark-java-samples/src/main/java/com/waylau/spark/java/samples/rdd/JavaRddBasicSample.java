/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.rdd;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * JavaRddBasic Sample.
 * 
 * @since 1.0.0 2021年7月25日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
public class JavaRddBasicSample {

	public static void main(String[] args) {
		// 要构建一个包含有Spark关应用程序信息的SparkConf对象
		SparkConf conf = new SparkConf()
				.setAppName("JavaRddBasicSample")// 设置应用名称
				.setMaster("local[4]"); // 本地4核运行

		// 创建一个JavaSparkContext对象，它告诉Spark如何访问群集
		JavaSparkContext sparkContext = new JavaSparkContext(
				conf);

		// 从现有集合上创建RDD
		List<Integer> existingCollection = Arrays.asList(1,
				2, 3, 4, 5);

		JavaRDD<Integer> rddFromExistingCollection =
				sparkContext
						.parallelize(existingCollection);

		// RDD的reduce操作
		int reduceResult = rddFromExistingCollection
				.reduce((a, b) -> a + b);

		System.out.println("reduceResult: " + reduceResult); // 15

		// 从外部存储系统源创建RDD
		JavaRDD<String> rddFromFextFile =
				sparkContext.textFile(
						"src/main/resources/JavaWordCountData.txt");

		// RDD的foreach操作，将文件内容打印到控制台
		rddFromFextFile
				.foreach(line -> System.out.println(line));

		// 关闭JavaSparkContext
		sparkContext.close();
	}

}
