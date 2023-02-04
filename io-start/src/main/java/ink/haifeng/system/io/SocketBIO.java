package ink.haifeng.system.io;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * SocketBIO
 *
 * @author haifeng
 * @version 2023/2/4 20:14
 */
public class SocketBIO {
    public static void main(String[] args) throws IOException {
        ServerSocket server = new ServerSocket(9090, 20);
        System.out.println("socket server");

        while (true) {
            Socket client = server.accept();
            System.out.println("client");

            new Thread(() -> {
                try {
                    InputStream in = client.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    while (true) {
                        String line = reader.readLine();
                        if (null != line) {
                            System.out.println(line);
                        } else {
                            client.close();
                            break;
                        }
                    }
                    System.out.println("client 断开");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start();
        }
    }
}
