package ink.haifeng.system.io;

import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * OSFileIo
 *
 * @author haifeng
 * @version 2023/1/30 21:30
 */
public class OSFileIO {


    private static byte[] data = "asdfasdfasdfasdfas\n".getBytes();
    private static String path = "./data.txt";

    public static void main(String[] args) {

    }

    /**
     * 测试最基本的文件io
     */
    @Test
    public void testBasicFileIO() throws IOException {
        File file = new File(path);
        FileOutputStream out = new FileOutputStream(file);
        out.write(data);
        out.flush();
    }

    @Test
    public void testBufferFileIO() throws IOException {
        File file = new File(path);
        BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(Paths.get(path)));
        out.write(data);
        out.flush();
    }

    @Test
    public void testRandomAccessFileWrite() throws IOException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        raf.write("hello Hadoop \n".getBytes());
        raf.write("hello Spark \n".getBytes());
        System.out.println("write data");
        // System.in.read();
        raf.seek(10);
        raf.write("test".getBytes());
        System.out.println("seek------");
        // System.in.read();
        FileChannel rafChannel = raf.getChannel();
        MappedByteBuffer map = rafChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        map.put("map buffer\n".getBytes());
        System.out.println("map buffer -----------");
        // System.in.read();
        raf.seek(0);
        ByteBuffer buffer = ByteBuffer.allocate(8192);

        int read = rafChannel.read(buffer);
        //System.out.println(buffer);
        buffer.flip();
        //System.out.println(buffer);

        for (int i = 0; i < buffer.limit(); i++) {
            //  Thread.sleep(200);
            System.out.print(((char) buffer.get(i)));
        }
    }

    @Test
    public void byteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        //ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        System.out.println("postition: " + buffer.position());
        System.out.println("limit: " + buffer.limit());
        System.out.println("capacity: " + buffer.capacity());
        System.out.println("mark: " + buffer);
        buffer.put("123".getBytes());
        System.out.println("-------------put:123......");
        System.out.println("mark: " + buffer);
        buffer.flip();   //读写交替
        System.out.println("-------------flip......");
        System.out.println("mark: " + buffer);
        buffer.get();
        System.out.println("-------------get......");
        System.out.println("mark: " + buffer);
        buffer.compact();
        System.out.println("-------------compact......");
        System.out.println("mark: " + buffer);
        buffer.clear();
        System.out.println("-------------clear......");
        System.out.println("mark: " + buffer);
    }
}
