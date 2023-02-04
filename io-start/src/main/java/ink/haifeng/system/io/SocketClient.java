package ink.haifeng.system.io;

import java.io.*;
import java.net.Socket;
import java.nio.Buffer;

/**
 * SocketClient
 *
 * @author haifeng
 * @version 2023/2/4 20:30
 */
public class SocketClient {
    public static void main(String[] args) throws IOException {
        Socket client = new Socket("192.168.233.129", 9000);
        client.setSendBufferSize(20);
        client.setTcpNoDelay(true);
        OutputStream out = client.getOutputStream();
        InputStream in = System.in;
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        while (true) {
            String line = reader.readLine();
            if (line != null) {
                byte[] bytes = line.getBytes();
                for (byte aByte : bytes) {
                    out.write(aByte);
                }
            }
        }
    }
}
