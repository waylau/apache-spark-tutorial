package com.waylau.spark.java.samples.rdd;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


/**
 * Java Word Count sample.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-05-28
 */
public class JavaWordCountSample {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        // 判断输入参数
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        // 初始化SparkSession
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JavaWordCountSample") // 设置应用名称
                .getOrCreate();

        // 读取文件内容，并转为RDD结构的文本
        JavaRDD<String> lines = sparkSession.read().textFile(args[0]).javaRDD();

        // 将文本行按照空格作为分隔，转成了一个单词列表
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        // 将列表里面的每个单词作为键、1作为值创建JavaPairRDD
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        // 将相同键的值做累加，从而得出了每个单词出现的次数，即词频。
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        // 收集结果，并打印
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        // 关闭SparkSession
        sparkSession.stop();
    }
}
