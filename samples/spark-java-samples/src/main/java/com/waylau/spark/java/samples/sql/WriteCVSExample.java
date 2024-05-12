package com.waylau.spark.java.samples.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.*;

import com.waylau.spark.java.samples.common.Person;

/**
 * Write CVS Example
 * 
 * @since 1.0.0 2021年7月19日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
public class WriteCVSExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("WriteCVS") // 设置应用名称
            .master("local") // 本地单线程运行
            .getOrCreate();

        // 创建Java Bean
        Person person01 = new Person();
        person01.setName("Way Lau");
        person01.setAge(35L);
        person01.setHomePage("https://waylau.com");

        Person person02 = new Person();
        person02.setName("Andy Huang");
        person02.setAge(25L);
        person02.setHomePage("https://waylau.com/books");

        List<Person> personList = new ArrayList<>();
        personList.add(person01);
        personList.add(person02);

        // 创建Java Bean的编码器
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        // 转为Dataset
        Dataset<Person> javaBeanListDS = sparkSession.createDataset(personList, personEncoder);

        // 导出为CSV文件
        javaBeanListDS.write().format("csv") // 文件格式
            .mode(SaveMode.Overwrite) // 如果第一次生成了，后续会覆盖
            .option("header", "true").save("target/output/people"); // 保存的文件所在的目录路径

        // 上述导出方式等同于下面的快捷方式：
        // 导出为CSV文件
        javaBeanListDS.write().mode(SaveMode.Overwrite) // 如果第一次生成了，后续会覆盖
            .option("header", "true").csv("target/output/people"); // 保存的文件所在的目录路径

        // 关闭SparkSession
        sparkSession.stop();
    }
}
