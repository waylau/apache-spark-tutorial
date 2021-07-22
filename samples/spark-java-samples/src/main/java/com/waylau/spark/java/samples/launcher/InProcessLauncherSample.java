/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.launcher;

import java.io.IOException;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.launcher.InProcessLauncher;

/**
 * InProcessLauncher Sample
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-07-22
 */

public class InProcessLauncherSample {

	public static void main(String[] args)
			throws IOException {
		new InProcessLauncher().setAppResource(
				"target/spark-java-samples-1.0.0.jar") // 应用程序
				.setMainClass(
						"com.waylau.spark.java.samples.sql.WriteCVSExample")// 启动函数入口
				.setMaster("local") // 集群配置
				.setConf(SparkLauncher.DRIVER_MEMORY, "2g") // 配置
				.startApplication();

		System.out.println("Done!");
	}

}