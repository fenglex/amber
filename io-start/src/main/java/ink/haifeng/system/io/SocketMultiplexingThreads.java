package ink.haifeng.system.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SocketMultiplexingThreads
 *
 * @author haifeng
 * @version 2023/2/5 19:17
 */
public class SocketMultiplexingThreads {

    private ServerSocketChannel server = null;
    private Selector selector1 = null;
    private Selector selector2 = null;
    private Selector selector3 = null;

    int port = 9000;

    public void initServer() {
        try {
            server = ServerSocketChannel.open();
            server.configureBlocking(false);
            server.bind(new InetSocketAddress(port));
            selector1 = Selector.open();
            selector2 = Selector.open();
            selector3 = Selector.open();
            server.register(selector1, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class NioThread extends Thread {
        Selector selector = null;
        int selectors = 0;
        int id = 0;
        volatile BlockingDeque<SocketChannel>[] queue;
        AtomicInteger idx = new AtomicInteger();

        NioThread(Selector selector, int n) {
            this.selector = selector;
            this.selectors = n;
            queue = new LinkedBlockingDeque[selectors];
            for (int i = 0; i < n; i++) {
                queue[i] = new LinkedBlockingDeque<>();
            }
            System.out.println("Boss 启动");
        }

        NioThread(Selector selector) {
            this.selector = selector;
            id = idx.getAndIncrement() % selectors;
            System.out.println("worker: " + id + " 启动");

        }

        @Override
        public void run() {
            try {
                while (true) {
                    while (selector.select(10) > 0) {
                        Set<SelectionKey> selectionKeys = selector.selectedKeys();
                        Iterator<SelectionKey> iterator = selectionKeys.iterator();
                        while (iterator.hasNext()) {
                            SelectionKey key = iterator.next();
                            iterator.remove();
                            if (key.isAcceptable()) {
                                acceptHandler(key);
                            } else if (key.isReadable()) {
                                readHandler(key);
                            }
                        }
                    }
                    if (!queue[id].isEmpty()) {
                        ByteBuffer buffer = ByteBuffer.allocate(8192);
                        SocketChannel client = queue[id].take();
                        client.register(selector, SelectionKey.OP_READ, buffer);
                        System.out.println("-------------------------------------------");
                        System.out.println("新客户端：" + client.socket().getPort() + "分配到：" + (id));
                        System.out.println("-------------------------------------------");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void acceptHandler(SelectionKey key) throws IOException {
            ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
            SocketChannel client = ssc.accept();
            client.configureBlocking(false);
            int num = idx.getAndIncrement() % selectors;
            queue[num].add(client);
        }


        public void readHandler(SelectionKey key) throws IOException {
            SocketChannel client = (SocketChannel) key.channel();
            ByteBuffer buffer = (ByteBuffer) key.attachment();
            buffer.clear();
            int read = 0;
            while (true) {
                read = client.read(buffer);
                if (read > 0) {
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        client.write(buffer);
                    }
                    buffer.clear();
                } else if (read == 0) {
                    break;
                } else {
                    client.close();
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        SocketMultiplexingThreads service = new SocketMultiplexingThreads();
        service.initServer();
        NioThread t1 = new NioThread(service.selector1, 2);
        NioThread t2 = new NioThread(service.selector2);
        NioThread t3 = new NioThread(service.selector3);
        t1.start();
        Thread.sleep(1000);
        t2.start();
        t3.start();
        System.out.println("服务器启动了");
        System.in.read();
    }

}
