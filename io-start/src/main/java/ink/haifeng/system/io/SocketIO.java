package ink.haifeng.system.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * SocketIO
 *
 * @author haifeng
 * @version 2023/2/4 21:23
 */
public class SocketIO {
    public static void main(String[] args) throws IOException {
        ServerSocket server = new ServerSocket(9000, 20);
        System.out.println("start server");

        while (true) {
            Socket client = server.accept();  //阻塞1
            System.out.println("step2:client\t" + client.getPort());
            new Thread(() -> {
                InputStream in = null;
                try {
                    in = client.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    while (true) {
                        String dataline = reader.readLine(); //阻塞2
                        if (null != dataline) {
                            System.out.println(dataline);
                        } else {
                            System.out.println("line null");
                            client.close();
                            break;
                        }
                    }
                    System.out.println("客户端断开");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}
