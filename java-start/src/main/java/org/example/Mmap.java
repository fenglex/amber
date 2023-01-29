package org.example;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Mmap
 * <p>
 * 用户进程将文件数据视为内存，因此无需发出read（）或write（）系统调用。
 * 当用户进程触摸映射的内存空间时，将自动生成页错误，以从磁盘引入文件数据。如果用户修改了映射的内存空间，则受影响的页面会自动标记为脏页面，随后将刷新到磁盘以更新文件。
 * 操作系统的虚拟内存子系统将执行页面的智能缓存，并根据系统负载自动管理内存。
 * 数据始终是页面对齐的，不需要复制缓冲区。
 * 可以映射非常大的文件，而无需消耗大量内存来复制数据。
 *
 * @author haifeng
 * @version 2023/1/6 18:21
 */
public class Mmap {

    @Test
    public void testRead() {
        String bigFile = "test.data";
        //Get file channel in read-only mode
        try (RandomAccessFile file = new RandomAccessFile(new File(bigFile), "r")) {
            FileChannel fileChannel = file.getChannel();
            //Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            // the buffer now reads the file as if it were loaded in memory.
            System.out.println(buffer.isLoaded());  //prints false
            System.out.println(buffer.capacity());  //Get the size based on content size of file
            //You can read the file from this buffer the way you like.
            for (int i = 0; i < buffer.limit(); i++) {
                System.out.println((char) buffer.get());
            }
        } catch (Exception f) {
            f.printStackTrace();
        }
    }

    @Test
    public void testWrite() {
        String  bigFile = "D:/newfile.txt";
        String  content = "java.com";
        File file = new File(bigFile);
        file.delete();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw")){
            FileChannel fileChannel = randomAccessFile.getChannel();
            // Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, content.length()+10);
            //Write the content using put methods
            buffer.put("java.com".getBytes());
        }catch(FileNotFoundException f){
            f.printStackTrace();
        }catch(IOException o){
            o.printStackTrace();
        }
    }
}
