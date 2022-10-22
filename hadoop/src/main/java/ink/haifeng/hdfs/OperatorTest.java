package ink.haifeng.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public class OperatorTest {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration conf = new Configuration(true);
        // conf.set("fs.DefaultFS","hdfs://cdh1:9000");
        // conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path("/user"));
        for (FileStatus fileStatus : status) {
            System.out.println(fileStatus.getPath() + "\t " + fileStatus.getPermission());
        }

        getFile("/tmp/test.file", fs);


        uploadFile(fs);
    }


    /**
     * 可以获取文件的元信息
     * 包括文件的属性信息，文件大小，文件每个块的位置，每个块的起始偏移量，这样就能做到移动计算不移动数据，
     * 根据文件所在的节点信息就能判断文件的数据本地化（数据的距离）
     * @param path
     * @param fs
     * @throws IOException
     */
    public static void getFile(String path, FileSystem fs) throws IOException {

        FileStatus fileStatus= fs.getFileStatus(new Path(path));
        System.out.println(fileStatus);

        BlockLocation[] locations = fs.getFileBlockLocations(new Path(path), 0, fileStatus.getLen());
        for (BlockLocation location : locations) {
            System.out.println(location);
        }

    }


    public static void uploadFile(FileSystem fs) throws IOException {
        Path from = new Path("/Users/haifeng/Workspace/projects/amber/hdfs-operator/src/main/resources/log4j.properties");
        Path to = new Path("/tmp/log4j.properties");
        fs.copyFromLocalFile(from,to);
    }

}
