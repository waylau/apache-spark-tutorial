/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * 持久化 示例
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 1.0.0 2024年5月11日
 */
public class JavaRddPersistenceSample {

    public static void main(String[] args) {
        // 要构建一个包含有Spark关应用程序信息的SparkConf对象
        SparkConf conf = new SparkConf().setAppName("JavaRddPersistenceSample")// 设置应用名称
                .setMaster("local[4]"); // 本地4核运行

        // 创建一个JavaSparkContext对象，它告诉Spark如何访问群集
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 创建一个包含字符串的JavaRDD
        List<String> bookList = Arrays.asList("分布式系统常用技术及案例分析",
                "Spring Boot 企业级应用开发实战",
                "Spring Cloud 微服务架构开发实战",
                "Spring 5 开发大全",
                "Cloud Native 分布式架构原理与实践",
                "Java核心编程",
                "轻量级Java EE企业应用开发实战",
                "鸿蒙HarmonyOS应用开发入门");
        JavaRDD<String> bookRDD = sparkContext.parallelize(bookList);

        // 持久化RDD，这里使用MEMORY_ONLY作为存储级别
        bookRDD.persist(StorageLevel.MEMORY_ONLY());

        // map转换用于将一个 RDD中的每个元素通过指定的函数转换成另一个RDD
        JavaRDD<Integer> bookLengths = bookRDD.map(s -> s.length());

        // collect动作获取结果并打印
        List<Integer> bookLengthsList = bookLengths.collect();
        for (Integer bookLength : bookLengthsList) {
            System.out.println(bookLength);
        }

        System.out.println("-----------------------------");

        // 假设我们再次需要原始RDD的数据，但由于它已经被持久化，所以不需要重新计算
        List<String> persistBookList = bookRDD.collect();

        // 打印结果
        for (String book : persistBookList) {
            System.out.println(book);
        }

        System.out.println("-----------------------------");

        // 取消持久化
        bookRDD.unpersist(true); // 阻塞

        // 关闭JavaSparkContext
        sparkContext.close();
    }

}
