package ink.haifeng.jetty;


import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import java.net.InetSocketAddress;

/**
 * JettyServer
 *
 * @author haifeng
 * @version 2023/1/29 21:35
 */
public class JettyServer {
    public static void main(String[] args) throws Exception {
        Server server = new Server(new InetSocketAddress("localhost", 9090));
        ServletContextHandler handler = new ServletContextHandler(server, "/");
        server.setHandler(handler);
        handler.addServlet(HttpHandler.class, "/*");  //web.xml

        // 放的位置决定是否会被触发
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("程序退出了啊");
        }));
        server.start();
        server.join();

    }


}
