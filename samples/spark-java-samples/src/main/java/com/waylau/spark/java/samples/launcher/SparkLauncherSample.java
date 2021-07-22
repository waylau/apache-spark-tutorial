/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.launcher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 * SparkLauncher Sample
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-07-22
 */
public class SparkLauncherSample {

	public static void main(String[] args)
			throws IOException, InterruptedException {

		SparkAppHandle handle = new SparkLauncher()
				.setAppResource(
						"target/spark-java-samples-1.0.0.jar") // 应用程序
				.setMainClass(
						"com.waylau.spark.java.samples.sql.WriteCVSExample")// 启动函数入口
				.setMaster("local") // 集群配置
				.setConf(SparkLauncher.DRIVER_MEMORY, "2g") // 配置
				.startApplication();

		CountDownLatch countDownLatch = new CountDownLatch(
				1);

		// 启动程序是异步的，所以要事件监听任务是否执行完成了
		handle.addListener(new SparkAppHandle.Listener() {
			@Override
			public void stateChanged(
					SparkAppHandle sparkAppHandle) {
				if (sparkAppHandle.getState().isFinal()) {
					// 启动程序完成允许结束
					countDownLatch.countDown();
				}

				System.out.println("state:" + sparkAppHandle
						.getState().toString());

			}

			@Override
			public void infoChanged(
					SparkAppHandle sparkAppHandle) {

				System.out.println("Info:" + sparkAppHandle
						.getState().toString());
			}

		});

		// 线程等待启动程序完成
		countDownLatch.await();
		
		System.out.println("Done!");
	}

}