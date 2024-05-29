/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * DataSource with Protobuf Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-05-28
 */
public class DataSourceProtobufExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DataSourceProtobufExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 构造数据
        List<Row> data = List.of(RowFactory.create(1, "John Doe", 30), RowFactory.create(2, "Jane Smith", 25));

        // 构造模式
        StructType schema =
            new StructType(new StructField[] {new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())});

        // 创建一个简单的DataFrame
        Dataset<Row> createDf = sparkSession.createDataFrame(data, schema);

        // 将DataFrame写入ORC文件
        createDf.write()
            // 文件格式ORC
            .format("orc")
            // 指定保存文件的路径
            .save("output/users_age.orc");

        // 创建DataFrame
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> readDf = sparkSession.read()
            // 文件格式ORC
            .format("orc")
            // 从指定路径读取文件
            .load("output/users_age.orc");

        // 将DataFrame的内容显示
        readDf.show();

        // 打印schema
        readDf.printSchema();

        // 关闭SparkSession
        sparkSession.stop();
    }

}