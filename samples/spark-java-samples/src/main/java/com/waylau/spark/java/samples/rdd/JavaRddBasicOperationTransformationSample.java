/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Transformation 示例
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 1.0.0 2024年5月10日
 */
public class JavaRddBasicOperationTransformationSample {

    public static void main(String[] args) {
        // 要构建一个包含有Spark关应用程序信息的SparkConf对象
        SparkConf conf = new SparkConf().setAppName("JavaRddBasicOperationTransformationSample")// 设置应用名称
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

        // 创建一个包含字符串的JavaRDD，包括一些空字符串
        List<String> bookWithEmptyList = Arrays.asList("分布式系统常用技术及案例分析",
                "",
                "Spring Cloud 微服务架构开发实战",
                "Spring 5 开发大全",
                null,
                "Java核心编程",
                "轻量级Java EE企业应用开发实战",
                "鸿蒙HarmonyOS应用开发入门");
        JavaRDD<String> bookWithEmptyRDD = sparkContext.parallelize(bookWithEmptyList);

        // 使用filter操作过滤出非空的字符串
        JavaRDD<String> nonEmptyLinesRDD = bookWithEmptyRDD.filter(line -> line != null && !line.isEmpty());

        // 收集结果并打印
        List<String> nonEmptyLines = nonEmptyLinesRDD.collect();
        for (String line : nonEmptyLines) {
            System.out.println(line);
        }

        System.out.println("-----------------------------");

        // flatMap转换类似于map，但每个输入项都可以映射到0个或多个输出项
        // 创建一个包含单词列表的 RDD
        List<List<String>> fruitList =
                Arrays.asList(Arrays.asList("apple", "banana"), Arrays.asList("orange", "grape"), Arrays.asList("kiwi"));

        JavaRDD<List<String>> rddFruitList = sparkContext.parallelize(fruitList);

        // 使用 flatMap 将每个列表的单词转换为一个 RDD
        JavaRDD<String> flatMapRDD = rddFruitList.flatMap(list -> list.iterator());

        // collect动作获取结果并打印
        List<String> fruitReusltList = flatMapRDD.collect();
        for (String fruit : fruitReusltList) {
            System.out.println(fruit);
        }

        System.out.println("-----------------------------");

        // 创建一个包含整数的JavaRDD
        List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sparkContext.parallelize(integerList);

        // 使用sample操作随机抽取一部分数据，这里抽取30%的数据，
        // 使用false作为第二个参数表示不使用替换（即无放回抽样）
        // 如果第二个参数设置为true，则表示使用替换（即有放回抽样）
        JavaRDD<Integer> sampledRDD = numbersRDD.sample(false, 0.3, 12345); // 12345是随机种子

        // 收集结果并打印
        List<Integer> sampledNumbers = sampledRDD.collect();
        for (Integer number : sampledNumbers) {
            System.out.println(number);
        }

        System.out.println("-----------------------------");

        // 创建两个List，每个List包含一些整数
        List<Integer> data1 = Arrays.asList(1, 2, 3);
        List<Integer> data2 = Arrays.asList(4, 5, 6);

        // 将List转换为JavaRDD
        JavaRDD<Integer> rdd1 = sparkContext.parallelize(data1);
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(data2);

        // union操作并两个RDD
        JavaRDD<Integer> unionedRDD = rdd1.union(rdd2);

        // 收集结果并打印
        List<Integer> unionedData = unionedRDD.collect();
        for (Integer number : unionedData) {
            System.out.println(number);
        }

        System.out.println("-----------------------------");

        // 创建一个包含重复元素的列表
        List<Integer> duplicatedIntegerList = Arrays.asList(1, 2, 2, 3, 4, 4, 5, 5, 5);

        // 将列表转换为RDD
        JavaRDD<Integer> duplicatedIntegerRDD = sparkContext.parallelize(duplicatedIntegerList);

        // 对RDD执行distinct操作，以消除重复项
        JavaRDD<Integer> distinctRDD = duplicatedIntegerRDD.distinct();

        // 收集结果并打印
        List<Integer> distinctIntegerList = distinctRDD.collect();
        for (Integer number : distinctIntegerList) {
            System.out.println(number);
        }

        System.out.println("-----------------------------");

        // 创建一个包含键值对的列表
        List<Tuple2<String, Integer>> tuple2List = Arrays.asList(new Tuple2<>("A", 1), new Tuple2<>("B", 2),
                new Tuple2<>("A", 3), new Tuple2<>("C", 4), new Tuple2<>("B", 5));

        // 将列表转换为JavaPairRDD
        JavaPairRDD<String, Integer> pairRDD = sparkContext.parallelizePairs(tuple2List);

        // 使用groupByKey()操作按键进行分组
        JavaPairRDD<String, Iterable<Integer>> groupedRDD = pairRDD.groupByKey();

        // 转换分组后的数据为列表形式以便查看（注意：在实际应用中可能不需要这样做）
        JavaRDD<String> resultRDD = groupedRDD.map(pair -> {
            StringBuilder sb = new StringBuilder();
            sb.append(pair._1()).append(": [");
            for (Integer value : pair._2()) {
                sb.append(value).append(", ");
            }
            if (pair._2().iterator().hasNext()) {
                // 移除最后一个逗号和空格
                sb.setLength(sb.length() - 2);
            }
            sb.append("]");
            return sb.toString();
        });

        // 收集结果并打印
        List<String> results = resultRDD.collect();
        for (String result : results) {
            System.out.println(result);
        }

        System.out.println("-----------------------------");

        // 关闭JavaSparkContext
        sparkContext.close();
    }

}
