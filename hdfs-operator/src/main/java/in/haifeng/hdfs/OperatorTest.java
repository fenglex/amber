package in.haifeng.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public class OperatorTest {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME","hdfs");
        Configuration conf = new Configuration(false);
        //conf.set("fs.DefaultFS","hdfs://cdh1:9000");
        //conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs=FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), false);
        while (files.hasNext()){
            LocatedFileStatus next = files.next();
            System.out.println(next.getPermission()+"" +
                    "t"+next.getPath());
        }

    }
}
