/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.util;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

/**
 * LongAccumulator Sample
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 1.0.0 2021年7月21日
 */
public class LongAccumulatorSample {

    public static void main(String[] args) {
        // 要构建一个包含有Spark关应用程序信息的SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("LongAccumulatorSample")// 设置应用名称
                .setMaster("local[4]"); // 本地4核运行

        // 创建一个JavaSparkContext对象，它告诉Spark如何访问群集
        JavaSparkContext sparkContext = new JavaSparkContext(
                conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        // 创建一个可以并行操作的分布式数据集
        JavaRDD<Integer> rdd = sparkContext
                .parallelize(data);

        /*
         *
         * // 以下操作累计Long值，是错误的！
         *
         * Long counter = 0L;
         *
         * rdd.foreach(x -> counter += x);
         *
         */

        // 来创建数字累加器用于累计Long值
        LongAccumulator counter = sparkContext.sc()
                .longAccumulator();

        // 累计Long值
        rdd.foreach(x -> counter.add(x));

        // 只有驱动程序才能读取累加器的值
        System.out.println(
                "Counter value: " + counter.value());

        // 关闭JavaSparkContext
        sparkContext.close();
    }

}
