/**
 * Welcome to https://waylau.com
 */

package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.SparkSession;
import java.io.File;

/**
 * DataSource with Hive Example.
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-05
 */
public class DataSourceHiveExample {

    public static void main(String[] args) {
        // warehouseLocation 指向托管数据库和表的仓库位置
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
     
        SparkSession sparkSession = SparkSession.builder()
            .appName("DataSourceHive") // 设置应用名称
            .master("local") // 本地单线程运行
            .config("spark.sql.warehouse.dir", warehouseLocation) // 设置仓库位置
            .enableHiveSupport() // 启用Hive支持
            .getOrCreate();

        // 建表
        sparkSession.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");

        // 从本地文件kv1.txt加载数据
        sparkSession.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");
     
        // 查询
        sparkSession.sql("SELECT * FROM src").show();
       
        // 关闭SparkSession
        sparkSession.stop();
    }

}