
/*
* Copyright (c) waylau.com, 2021. All rights reserved.
*/
package com.waylau.spark.java.samples.ml;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;

/**
 * Estimator Transformer Param Example.
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-20
 */
public class EstimatorTransformerParamExample {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder()
				.appName("EstimatorTransformerParam") // 设置应用名称
				.master("local") // 本地单线程运行
				.getOrCreate();

		// 准备训练数据
		List<Row> dataTraining = Arrays.asList(
				RowFactory.create(1.0,
						Vectors.dense(0.0, 1.1, 0.1)),
				RowFactory.create(0.0,
						Vectors.dense(2.0, 1.0, -1.0)),
				RowFactory.create(0.0,
						Vectors.dense(2.0, 1.3, 1.0)),
				RowFactory.create(1.0,
						Vectors.dense(0.0, 1.2, -0.5))

		);

		StructType schema = new StructType(
				new StructField[] {
						new StructField("label",
								DataTypes.DoubleType, false,
								Metadata.empty()),

						new StructField("features",
								new VectorUDT(), false,
								Metadata.empty())
				});

		Dataset<Row> training = sparkSession
				.createDataFrame(dataTraining, schema);

		// 创建一个LogisticRegression实例lr，该实例是一个Estimator
		LogisticRegression lr = new LogisticRegression();

		// 打印lr参数
		System.out
				.println("LogisticRegression parameters:\n"
						+ lr.explainParams() + "\n");

		// 设置lr参数
		lr.setMaxIter(10).setRegParam(0.01);

		// 学习LogisticRegression模型。这个模型使用存储在lr中的参数
		LogisticRegressionModel model1 = lr.fit(training);

		// 打印model1参数
		System.out.println(
				"Model 1 was fit using parameters: "
						+ model1.parent()
								.extractParamMap());

		// 也可以使用ParamMap指定参数
		ParamMap paramMap = new ParamMap()
				.put(lr.maxIter().w(20)) // 指定参数
				.put(lr.maxIter(), 30) // 覆盖已有的参数
				.put(lr.regParam().w(0.1),
						lr.threshold().w(0.55)); // 指定多个参数

		// 可以合并ParamMap参数
		ParamMap paramMap2 = new ParamMap()
				.put(lr.probabilityCol()
						.w("myProbability")); // 更改输出列名

		ParamMap paramMapCombined = paramMap
				.$plus$plus(paramMap2);

		// 现在使用paramMapCombined参数学习一个新模型。
		// paramMapCombined覆盖之前通过lr.set*方法设置的所有参数。
		LogisticRegressionModel model2 = lr.fit(training,
				paramMapCombined);

		System.out.println(
				"Model 2 was fit using parameters: "
						+ model2.parent()
								.extractParamMap());

		// 准备测试文档
		List<Row> dataTest = Arrays.asList(
				RowFactory.create(1.0,
						Vectors.dense(-1.0, 1.5, 1.3)),
				RowFactory.create(0.0,
						Vectors.dense(3.0, 2.0, -0.1)),
				RowFactory.create(1.0,
						Vectors.dense(0.0, 2.2, -1.5))
		);

		Dataset<Row> test = sparkSession
				.createDataFrame(dataTest, schema);

		// 使用Transformer.transform()方法对测试文档进行预测。
		// LogisticRegression.transform将仅使用“features”列。
		// 请注意，model2.transform()输出一个“myProbability”列，而不是通常的“probability”列，
		// 因为我们之前重命名了lr.probabilityCol参数。
		Dataset<Row> results = model2.transform(test);
		Dataset<Row> rows = results.select("features",
				"label", "myProbability", "prediction");

		for (Row r : rows.collectAsList()) {
			System.out.println("(" + r.get(0) + ", "
					+ r.get(1) + ") -> prob=" + r.get(2)
					+ ", prediction=" + r.get(3));

		}

		// 关闭SparkSession
		sparkSession.stop();

	}

}