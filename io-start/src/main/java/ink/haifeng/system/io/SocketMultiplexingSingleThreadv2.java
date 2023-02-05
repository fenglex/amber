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

/**
 * SocketMultiplexingSingleThreadv2
 *
 * @author haifeng
 * @version 2023/2/5 19:42
 */
public class SocketMultiplexingSingleThreadv2 {

    private ServerSocketChannel server = null;
    private Selector selector = null;
    int port = 9000;

    public void initServer() throws IOException {
        server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(port));
        selector = Selector.open();
        server.register(selector, SelectionKey.OP_ACCEPT);
    }


    public void start() throws IOException {
        initServer();
        System.out.println("服务器启动了 。。。。。。。。。");
        while (true) {
            while (selector.select(50) > 0) {
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if (key.isAcceptable()) {
                        acceptHandler(key);
                    } else if (key.isReadable()) {
//                            key.cancel();  //现在多路复用器里把key  cancel了
                        System.out.println("in.....");
                        key.interestOps(key.interestOps() | ~SelectionKey.OP_READ);
                        readHandler(key);//还是阻塞的嘛？ 即便以抛出了线程去读取，但是在时差里，这个key的read事件会被重复触发
                    } else if (key.isWritable()) {
                        //我之前没讲过写的事件！！！！！
                        //写事件<--  send-queue  只要是空的，就一定会给你返回可以写的事件，就会回调我们的写方法
                        //你真的要明白：什么时候写？不是依赖send-queue是不是有空间
                        //1，你准备好要写什么了，这是第一步
                        //2，第二步你才关心send-queue是否有空间
                        //3，so，读 read 一开始就要注册，但是write依赖以上关系，什么时候用什么时候注册
                        //4，如果一开始就注册了write的事件，进入死循环，一直调起！！！
//                            key.cancel();
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);

                    }
                }
            }

        }
    }

    public void acceptHandler(SelectionKey key) throws IOException {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel client = ssc.accept();
        client.configureBlocking(false);
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        client.register(selector, SelectionKey.OP_READ, buffer);
        System.out.println("-------------------------------------------");
        System.out.println("新客户端：" + client.getRemoteAddress());
        System.out.println("-------------------------------------------");
    }

    public void readHandler(SelectionKey key) {
        new Thread(() -> {
            System.out.println("read handler ------");
            SocketChannel client = (SocketChannel) key.channel();
            ByteBuffer buffer = (ByteBuffer) key.attachment();
            buffer.clear();
            int read = 0;
            while (true) {
                try {
                    read = client.read(buffer);
                    System.out.println(Thread.currentThread().getName() + " " + read);
                    if (read > 0) {
                        key.interestOps(SelectionKey.OP_READ);
                        client.register(key.selector(), SelectionKey.OP_WRITE, buffer);
                    } else if (read == 0) {
                        break;
                    } else {
                        client.close();
                        break;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    public static void main(String[] args) throws IOException {
        SocketMultiplexingSingleThreadv2 service = new SocketMultiplexingSingleThreadv2();
        service.start();
    }
}
