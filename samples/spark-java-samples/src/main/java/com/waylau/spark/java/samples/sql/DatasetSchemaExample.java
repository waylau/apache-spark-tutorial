/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.sql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import com.waylau.spark.java.samples.common.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;

/**
 * Dataset Schema Example.
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-02
 */

public class DatasetSchemaExample {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder()
				.appName("DatasetSchema") // 设置应用名称
				.master("local") // 本地单线程运行
				.getOrCreate();

		// 从JSON文件创建Person对象的RDD
		JavaRDD<Person> peopleRDD = sparkSession.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.json("src/main/resources/people.json") // 加载存储JSON对象的Dataset
				.javaRDD() // Dataset转为JavaRDD
				.map(line -> {
					Person person = new Person();
					person.setName(line.getAs("name"));
					person.setAge(line.getAs("age"));
					person.setHomePage(
							line.getAs("homePage"));
					return person; // 转为Person对象的RDD
				});

		// 将schema应用于JavaBean的RDD以获得DataFrame
		Dataset<Row> peopleDataFrameSchemaFromJavaBean = sparkSession
				.createDataFrame(peopleRDD, Person.class);

		// 将DataFrame的内容显示
		peopleDataFrameSchemaFromJavaBean.show();

		// 从JSON文件创建RDD
		JavaRDD<Row> rowRDD = sparkSession.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.json("src/main/resources/people.json") // 加载存储JSON对象的Dataset
				.javaRDD() // Dataset转为JavaRDD
				.map(line -> {
					return RowFactory.create(
							line.getAs("name"),
							line.getAs("age"),
							line.getAs("homePage")); // 转为Row对象的RDD
				});

		// 编程方式定义schema
		List<StructField> fields = new ArrayList<>();
		StructField fieldName = DataTypes.createStructField(
				"name", DataTypes.StringType, true);
		StructField fieldAge = DataTypes.createStructField(
				"age", DataTypes.LongType, true);
		StructField fieldHomePage = DataTypes
				.createStructField("homePage",
						DataTypes.StringType, true);
		fields.add(fieldName);
		fields.add(fieldAge);
		fields.add(fieldHomePage);

		StructType structType = DataTypes
				.createStructType(fields);

		// 将schema应用于Row的RDD以获得DataFrame
		Dataset<Row> peopleDataFrameSchemaFromStructType =

				sparkSession.createDataFrame(rowRDD,
						structType);

		// 将DataFrame的内容显示
		peopleDataFrameSchemaFromStructType.show();

		// 关闭SparkSession
		sparkSession.stop();
	}

}