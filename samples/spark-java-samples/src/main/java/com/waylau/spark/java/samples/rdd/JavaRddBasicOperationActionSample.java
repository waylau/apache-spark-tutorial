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
 * Action 示例
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 1.0.0 2024年5月11日
 */
public class JavaRddBasicOperationActionSample {

    public static void main(String[] args) {
        // 要构建一个包含有Spark关应用程序信息的SparkConf对象
        SparkConf conf = new SparkConf().setAppName("JavaRddBasicOperationActionSample")// 设置应用名称
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

        // map转换用于将一个 RDD中的每个元素通过指定的函数转换成另一个RDD
        JavaRDD<Integer> bookLengths = bookRDD.map(s -> s.length());

        // collect动作获取结果并打印
        List<Integer> bookLengthsList = bookLengths.collect();
        for (Integer bookLength : bookLengthsList) {
            System.out.println(bookLength);
        }

        System.out.println("-----------------------------");

        // 创建一个包含一些整数的列表
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 将列表转换为JavaRDD
        JavaRDD<Integer> rdd = sparkContext.parallelize(numbers);

        // 使用reduce操作计算所有数字的总和
        Integer sum = rdd.reduce(Integer::sum);

        // 打印结果
        System.out.println(sum);

        System.out.println("-----------------------------");

        // 创建一个包含一些字符串的列表
        List<String> fruitList = Arrays.asList("apple", "banana", "cherry", "date", "elderberry");

        // 将列表转换为JavaRDD
        JavaRDD<String> fruitRdd = sparkContext.parallelize(fruitList);

        // 使用count操作计算RDD中的元素数量
        long count = fruitRdd.count();

        // 打印结果
        System.out.println(count);

        System.out.println("-----------------------------");

        // 创建一个包含整数的JavaRDD
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sparkContext.parallelize(integerList);

        // 使用first操作获取RDD中的第一个元素
        Integer firstNumber = numbersRDD.first();

        // 打印结果
        System.out.println(firstNumber);

        System.out.println("-----------------------------");

        // 使用take操作取出前3个元素
        List<String> firstThreebookList = bookRDD.take(3);

        // 打印结果
        for (String book : firstThreebookList) {
            System.out.println(book);
        }

        System.out.println("-----------------------------");

        // 使用foreach操作打印每本书
        bookRDD.foreach(book -> System.out.println(book));

        System.out.println("-----------------------------");

        // 使用saveAsTextFile方法将RDD保存为文本文件
        // 注意：saveAsTextFile会创建一个目录，文件将保存在该目录下，每个分区一个文件
        String outputPath = "output/textBookRDD"; // 指定输出目录
        bookRDD.saveAsTextFile(outputPath);

        System.out.println("-----------------------------");

        // 关闭JavaSparkContext
        sparkContext.close();
    }

}
