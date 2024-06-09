package com.waylau.spark.java.samples.streaming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka Producer
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-05-31
 */
public class CustomKafkaProducer {
    /**
     * 定义主题
     */
    private static String TOPIC = "test_topic";

    public static void main(String[] args) throws Exception {
        // 构造生产者
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.78:9092");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p)) {
            // 持续发送数据
            int size = 1000;
            for (int i = 0; i < size; i++) {
                // 构造消息
                String msg = "Hi, " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, msg);
                kafkaProducer.send(record);
                System.out.println("send: " + msg);
                Thread.sleep(1000);
            }
        }
        // try语句会自动关闭生产者

    }
}
