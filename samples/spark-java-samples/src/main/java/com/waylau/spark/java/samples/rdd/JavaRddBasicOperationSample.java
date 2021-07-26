/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
/**
 * @since 1.0.0 2021年7月26日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
public class JavaRddBasicOperationSample {

	public static void main(String[] args) {
		// 要构建一个包含有Spark关应用程序信息的SparkConf对象

		SparkConf conf = new SparkConf()
		.setAppName("LongAccumulator")// 设置应用名称
		.setMaster("local[4]"); // 本地4核运行

		// 创建一个JavaSparkContext对象，它告诉Spark如何访问群集
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		// 从外部存储系统源创建RDD
		JavaRDD<String> rddFromFextFile =
		sparkContext.textFile("src/main/resources/JavaWordCountData.txt");

		// Transformation
		JavaRDD<Integer> lineLengths = rddFromFextFile.map(s -> s.length());

		// Action
		int totalLength = lineLengths.reduce((a, b) -> a + b);

		// 等同于下面的方式
		/*
		JavaRDD<Integer> lineLengths = rddFromFextFile.map(new Function<String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(String s) {
				eturn s.length();
			}

		});

		int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});
		*/

		// 持久化到内存中
		lineLengths.persist(StorageLevel.MEMORY_ONLY());

		System.out.println("totalLength: " + totalLength);

		// 关闭JavaSparkContext
		sparkContext.close();

	}

}
