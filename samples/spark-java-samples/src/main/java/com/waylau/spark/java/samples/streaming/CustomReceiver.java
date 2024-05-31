package com.waylau.spark.java.samples.streaming;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * Custom Receiver
 *
 * @author <a href="https://waylau.com">Way Lau</a>
 * @since 2024-05-31
 */
public class CustomReceiver extends Receiver<String> {

    String host = null;
    int port = -1;

    public CustomReceiver(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    @Override
    public void onStart() {
        // 启动通过连接接收数据的线程
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {

    }

    /** 创建套接字连接并接收数据，直到Receiver停止 */
    private void receive() {
        Socket socket = null;
        String userInput = null;

        try {
            // 连接到服务器
            socket = new Socket(host, port);

            BufferedReader reader =
                new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            // 持续读取数据直到停止或连接断开
            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");
                store(userInput);
            }
            reader.close();
            socket.close();

            // 重新启动以尝试在服务器再次处于活动状态时重新连接
            restart("Trying to connect again");
        } catch (ConnectException ce) {
            // 如果无法连接到服务器时则重启
            restart("Could not connect", ce);
        } catch (Throwable t) {
            // 有任何错误则重启
            restart("Error receiving data", t);
        }
    }
}
