
/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Word Sender
 * 
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2021-08-09
 */
public class WordSender {

	public static int PORT = 9999;

	public static void main(String[] args) {
		ServerSocket serverSocket = null;
		try {
			// 服务器监听
			serverSocket = new ServerSocket(PORT);
			System.out.println("WordSender已启动，端口：" + PORT);
		} catch (IOException e) {
			System.out.println("WordSender启动异常，端口：" + PORT);
			System.out.println(e.getMessage());
		}

		// Java 7 try-with-resource语句
		try (
				// 接受客户端建立链接，生成Socket实例
				Socket clientSocket = serverSocket.accept();

				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(),
						true);

				// 接收客户端的信息
				BufferedReader in = new BufferedReader(
						new InputStreamReader(clientSocket
								.getInputStream()));) {

			// 每1秒发送一次数据，不断发送
			while (true) {
				try {
					Thread.sleep(1000); // 暂停1秒
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// 获取当前时间
				char word = (char) randomChar();

				// 发送信息给客户端
				out.println(word);
				System.out.println("WordSender -> "
						+ clientSocket
								.getRemoteSocketAddress()
						+ ":" + word);
			}
		} catch (IOException e) {
			System.out.println(
					"WordSender异常!" + e.getMessage());
		}
	}
	
	//生成随机字符
	private static byte randomChar() {
		int flag = (int) (Math.random() * 2);// 0小写字母1大写字母
		byte resultBt;

		if (flag == 0) {
			byte bt = (byte) (Math.random() * 26);// 0 <= bt < 26
			resultBt = (byte) (65 + bt);
		} else {
			byte bt = (byte) (Math.random() * 26);// 0 <= bt < 26
			resultBt = (byte) (97 + bt);
		}

		return resultBt;
	}

}