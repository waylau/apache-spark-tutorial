/*
* Copyright (c) waylau.com, 2021. All rights reserved.
*/
package com.waylau.spark.java.samples.rdd;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag;

/**
 * Java RDD GraphX sample.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-25
 */
public class JavaRddGraphXSample {

	public static void main(String[] args) {
		// 要构建一个包含有Spark关应用程序信息的SparkConf对象
		SparkConf conf = new SparkConf()
				.setAppName("JavaRddGraphXSample")// 设置应用名称
				.setMaster("local[4]"); // 本地4核运行

		// 创建一个JavaSparkContext对象，它告诉Spark如何访问群集
		JavaSparkContext sparkContext = new JavaSparkContext(
				conf);

		// 初始化Edge，属性是String类型
		List<Edge<String>> edges = new ArrayList<>();
		edges.add(new Edge<String>(1, 2, "Friend1"));
		edges.add(new Edge<String>(2, 3, "Friend2"));
		edges.add(new Edge<String>(1, 3, "Friend3"));
		edges.add(new Edge<String>(4, 3, "Friend4"));
		edges.add(new Edge<String>(4, 5, "Friend5"));
		edges.add(new Edge<String>(2, 5, "Friend6"));

		JavaRDD<Edge<String>> edgeRDD = sparkContext
				.parallelize(edges);

		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$
				.apply(String.class);

		// 从Edge初始化Graph
		Graph<String, String> graph = Graph.fromEdges(
				edgeRDD.rdd(),
				"v", // 默认Vertex属性值
				StorageLevel.MEMORY_ONLY(), // 存储级别为内存
				StorageLevel.MEMORY_ONLY(), // 存储级别为内存
				stringTag, // 属性是String类型
				stringTag); // 属性是String类型

		// 打印Vertex
		graph.vertices().toJavaRDD().collect()
				.forEach(System.out::println);

		// 关闭JavaSparkContext
		sparkContext.close();
	}

}