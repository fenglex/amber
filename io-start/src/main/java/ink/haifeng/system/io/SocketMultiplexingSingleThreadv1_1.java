package ink.haifeng.system.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * SocketMultiplexingSingleThreadv1_1
 *
 * @author haifeng
 * @version 2023/2/5 18:54
 */
public class SocketMultiplexingSingleThreadv1_1 {

    private ServerSocketChannel server = null;
    private Selector selector = null;
    int port = 9000;

    public void initServer() {
        try {
            server = ServerSocketChannel.open();
            server.configureBlocking(false);
            server.bind(new InetSocketAddress(port));
            selector = Selector.open();
            server.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() throws IOException {
        initServer();
        System.out.println("启动服务器");
        while (true) {
            while (selector.select() > 0) {
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if (key.isAcceptable()) {
                        acceptHandler(key);
                    } else if (key.isReadable()) {
                        key.cancel();
                        readHandler(key);  //只处理了  read  并注册 关心这个key的write事件

                    } else if (key.isWritable()) {
                        //写事件<--  send-queue  只要是空的，就一定会给你返回可以写的事件，就会回调我们的写方法
                        //你真的要明白：你想什么时候写？不是依赖send-queue是不是有空间（多路复用器能不能写是参考send-queue有没有空间）
                        //1，你准备好要写什么了，这是第一步
                        //2，第二步你才关心send-queue是否有空间
                        //3，so，读 read 一开始就要注册，但是write依赖以上关系，什么时候用什么时候注册
                        //4，如果一开始就注册了write的事件，进入死循环，一直调起！！！
//                            key.cancel();
                        writeHandler(key);
                    }
                }
            }
        }
    }


    public void acceptHandler(SelectionKey key) {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        try {
            SocketChannel client = ssc.accept();
            client.configureBlocking(false);
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            client.register(selector, SelectionKey.OP_READ, buffer);
            System.out.println("-------------------------------------------");
            System.out.println("新客户端：" + client.getRemoteAddress());
            System.out.println("-------------------------------------------");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void readHandler(SelectionKey key) throws IOException {
        System.out.println("read handler.....");
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        buffer.clear();
        int read = 0;
        while (true) {
            read = client.read(buffer);
            if (read > 0) {
                client.register(key.selector(), SelectionKey.OP_WRITE, buffer);
            } else if (read == 0) {
                break;
            } else {
                client.close();
                break;
            }
        }
    }

    public void writeHandler(SelectionKey key) {
        System.out.println("write handler...");
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        buffer.flip();
        while (buffer.hasRemaining()) {
            try {
                client.write(buffer);
                Thread.sleep(1000);
                buffer.clear();
                key.cancel();
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        SocketMultiplexingSingleThreadv1_1 service = new SocketMultiplexingSingleThreadv1_1();
        service.start();
    }
}
