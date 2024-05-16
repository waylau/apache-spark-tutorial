/*
 * Copyright (c) waylau.com, 2021. All rights reserved.
 */
package com.waylau.spark.java.samples.ml;

import java.util.Arrays;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.waylau.spark.java.samples.common.JavaDocument;
import com.waylau.spark.java.samples.common.JavaLabeledDocument;

/**
 * Pipeline Example
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-20
 */
public class PipelineExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                // 设置应用名称
                .appName("PipelineExample")
                // 本地单线程运行
                .master("local")
                .getOrCreate();

        // 准备带标签的训练文档
        Dataset<Row> training = sparkSession
                .createDataFrame(Arrays.asList(
                        new JavaLabeledDocument(0L,
                                "a b c d e spark", 1.0),
                        new JavaLabeledDocument(1L, "b d",
                                0.0),
                        new JavaLabeledDocument(2L,
                                "spark f g h", 1.0),
                        new JavaLabeledDocument(3L,
                                "hadoop mapreduce", 0.0)
                ), JavaLabeledDocument.class);

        // 配置ML Pipeline，由tokenizer、hashingTF、lr三个阶段组成
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");

        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001);

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer,
                        hashingTF, lr});

        // 学习PipelineModel
        PipelineModel model = pipeline.fit(training);

        // 准备不带标签的训练文档
        Dataset<Row> test = sparkSession
                .createDataFrame(Arrays.asList(
                        new JavaDocument(4L, "spark i j k"),
                        new JavaDocument(5L, "l m n"),
                        new JavaDocument(6L,
                                "spark hadoop spark"),
                        new JavaDocument(7L,
                                "apache hadoop")
                ), JavaDocument.class);

        // 对测试文档进行预测
        Dataset<Row> predictions = model.transform(test);
        for (Row r : predictions.select("id", "text",
                        "probability", "prediction")
                .collectAsList()) {
            System.out.println("(" + r.get(0) + ", "
                    + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }

        // 关闭SparkSession
        sparkSession.stop();
    }

}