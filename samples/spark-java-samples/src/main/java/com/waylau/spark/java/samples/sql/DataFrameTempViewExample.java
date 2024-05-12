/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataFrame Temp View Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-02
 */

public class DataFrameTempViewExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
            // 设置应用名称
            .appName("DatasetBasicExample")
            // 本地单线程运行
            .master("local").getOrCreate();

        // 创建DataFrame
        // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
        Dataset<Row> df = sparkSession.read()
            // 加载存储JSON对象的Dataset
            .json("people.json");

        // 使用createTempView或createOrReplaceTempView创建本地临时视图
        df.createTempView("v_people");

        // 是否已经有了同名的本地临时视图，如果存在则会覆盖掉已有的视图
        df.createOrReplaceTempView("v_people");

        // 在临时视图使用SQL查询
        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM v_people");

        // 将DataFrame的内容显示
        sqlDF.show();

        // 打印schema
        sqlDF.printSchema();

        // 指定列名来查询相应列的数据
        Dataset<Row> sqlDFWithName = sparkSession.sql("SELECT name FROM v_people");

        sqlDFWithName.show();

        // 上述等同于
        df.select("name").show();

        // 使用createGlobalTempView或createOrReplaceGlobalTempView创建全局临时视图
        df.createGlobalTempView("v_people");

        // 是否已经有了同名的全局临时视图，如果存在则会覆盖掉已有的视图
        df.createOrReplaceGlobalTempView("v_people");

        // 指定列名来查询相应列的数据
        Dataset<Row> sqlDFWithHostPage =

            sparkSession.sql("SELECT homePage FROM global_temp.v_people");

        // 将DataFrame的内容显示
        sqlDFWithHostPage.show();

        // 关闭SparkSession
        sparkSession.stop();
    }

}