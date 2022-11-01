package ink.haifeng.mr.temperature;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TopN {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.app-submission.cross-platform","true");
         //conf.set("hadoop.home.dir","D:\\Program Files\\hadoop-2.6.5");
        Job job = Job.getInstance(conf);
        job.setJarByClass(TopN.class);
        job.setJobName("TopN");
        // job.setJar("//");

        // maptask
        // input output
        Path input = new Path("file:///D:\\Workspace\\amber\\hadoop\\data\\temperature.txt");
        TextInputFormat.addInputPath(job,input);
        Path output = new Path("file:///D:\\Workspace\\amber\\hadoop\\data\\output");
        if (output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output,true);
        }
        TextOutputFormat.setOutputPath(job,output);
        // key
        // map
        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(TempInfo.class);
        job.setMapOutputValueClass(IntWritable.class);
        // partitioner 按年月分区，
        job.setPartitionerClass(TopNPartitoner.class);
        job.setSortComparatorClass(TopNSortComparator.class);
        job.setGroupingComparatorClass(TopNGroupComparator.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        // 添加缓存文件
        // job.setCacheFiles();
        

        job.waitForCompletion(true);

    }
}
