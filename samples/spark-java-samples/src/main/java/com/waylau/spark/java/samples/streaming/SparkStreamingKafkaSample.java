/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * SparkStreaming Kafka Sample
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-05-31
 */

public class SparkStreamingKafkaSample {
    /**
     * 定义主题
     */
    private static String TOPIC = "test_topic";

    public static void main(String[] args) throws InterruptedException {
        // 配置
        SparkConf conf = new SparkConf()
            // 本地多线程运行。不能设置成单线程
            .setMaster("local[*]")
            // 设置应用名称
            .setAppName("SparkStreamingKafkaSample");

        // Streaming上下文，每隔10秒执行一次获取批量数据然后处理这些数据
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // 定义Kafka参数
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");

        Collection<String> topics = Arrays.asList(TOPIC);// 配置topic，可以是数组

        JavaInputDStream<ConsumerRecord<String, String>> javaInputDStream =
            KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> javaPairDStream =
            javaInputDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, String> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                    return new Tuple2<>(consumerRecord.key(), consumerRecord.value());
                }
            });

        javaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> javaPairRDD) throws Exception {
                // TODO Auto-generated method stub
                javaPairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
                    @Override
                    public void call(Tuple2<String, String> tuple2) throws Exception {
                        // TODO Auto-generated method stub
                        System.out.println(tuple2._2);
                    }
                });
            }
        });

        /*JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(javaStreamingContext,
            LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        
        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        });*/

        // 启动JavaStreamingContext
        javaStreamingContext.start();

        // 等待JavaStreamingContext被中断
        javaStreamingContext.awaitTermination();

        // 关闭
        javaStreamingContext.close();
    }

}