/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.waylau.spark.java.samples.common.Person;

/**
 * Dataset Schema Example.
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-02
 */

public class DatasetSchemaExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DatasetSchemaExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 从JSON文件创建Person对象的RDD
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        JavaRDD<Person> peopleRDD = sparkSession.read()
            // 加载存储JSON对象的Dataset
            .json("people.json")
            // Dataset转为JavaRDD
            .javaRDD().map(line -> {
                Person person = new Person();
                person.setName(line.getAs("name"));
                person.setAge(line.getAs("age"));
                person.setHomePage(line.getAs("homePage"));
                return person; // 转为Person对象的RDD
            });

        // 将schema应用于JavaBean的RDD以获得DataFrame
        Dataset<Row> peopleDataFrameSchemaFromJavaBean = sparkSession.createDataFrame(peopleRDD, Person.class);

        // 将DataFrame的内容显示
        peopleDataFrameSchemaFromJavaBean.show();

        // 从JSON文件创建RDD
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        JavaRDD<Row> rowRDD = sparkSession.read()
            // 加载存储JSON对象的Dataset
            .json("people.json")
            // Dataset转为JavaRDD
            .javaRDD().map(line -> {
                // 转为Row对象的RDD
                return RowFactory.create(line.getAs("name"), line.getAs("age"), line.getAs("homePage"));
            });

        // 编程方式定义schema
        List<StructField> fields = new ArrayList<>();
        StructField fieldName = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField fieldAge = DataTypes.createStructField("age", DataTypes.LongType, true);
        StructField fieldHomePage = DataTypes.createStructField("homePage", DataTypes.StringType, true);
        fields.add(fieldName);
        fields.add(fieldAge);
        fields.add(fieldHomePage);

        StructType structType = DataTypes.createStructType(fields);

        // 将schema应用于Row的RDD以获得DataFrame
        Dataset<Row> peopleDataFrameSchemaFromStructType = sparkSession.createDataFrame(rowRDD, structType);

        // 将DataFrame的内容显示
        peopleDataFrameSchemaFromStructType.show();

        // 关闭SparkSession
        sparkSession.stop();
    }

}