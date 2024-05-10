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
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 1.0.0 2021年7月26日
 */
public class JavaRddBasicOperationSample {

    public static void main(String[] args) {
        // 要构建一个包含有Spark关应用程序信息的SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("JavaRddBasicOperationSample")// 设置应用名称
                .setMaster("local[4]"); // 本地4核运行

        // 创建一个JavaSparkContext对象，它告诉Spark如何访问群集
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 从外部存储系统源创建RDD
        JavaRDD<String> rddFromFextFile =
                sparkContext.textFile("src/main/resources/JavaWordCountData.txt");

        // map转换用于将一个 RDD中的每个元素通过指定的函数转换成另一个RDD
        JavaRDD<Integer> lineLengths = rddFromFextFile.map(s -> s.length());
        // 等同于下面的方式
		/*
		JavaRDD<Integer> lineLengths = rddFromFextFile.map(new Function<String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(String s) {
				return s.length();
			}

		});

		*/

        // collect动作获取结果并打印
        List<Integer> lineLengthsList = lineLengths.collect();
        for (Integer lineLength : lineLengthsList) {
            System.out.println(lineLength);
        }


        // flatMap转换类似于map，但每个输入项都可以映射到0个或多个输出项
        // 创建一个包含单词列表的 RDD
        List<List<String>> fruitList = Arrays.asList(
                Arrays.asList("apple", "banana"),
                Arrays.asList("orange", "grape"),
                Arrays.asList("kiwi")
        );

        JavaRDD<List<String>> rddFruitList = sparkContext.parallelize(fruitList);

        // 使用 flatMap 将每个列表的单词转换为一个 RDD
        JavaRDD<String> flatMapRDD = rddFruitList.flatMap(list -> list.iterator());

        // collect动作获取结果并打印
        List<String> fruitReusltList = flatMapRDD.collect();
        for (String fruit : fruitReusltList) {
            System.out.println(fruit);
        }

        // Action
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        // 等同于下面的方式
		/*
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
