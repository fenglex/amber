package ink.haifeng.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordSortJob {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(true);
        // GenericOptionsParser parser = new GenericOptionsParser(conf, args);  //工具类帮我们把-D 等等的属性直接set到conf，会留下commandOptions
        //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.framework.name","local");
       System.out.println(conf.get("mapreduce.framework.name"));
        Job job = Job.getInstance(conf);
//        FileInputFormat.setMinInputSplitSize(job,2222);
//        job.setInputFormatClass(ooxx.class);
       // job.setJar("C:\\Users\\admin\\IdeaProjects\\msbhadoop\\target\\hadoop-hdfs-1.0-0.1.jar");
        //必须必须写的
        job.setJarByClass(WordSortJob.class);
        job.setJobName("wordcount");
        // 可以自定义排序
        //job.setSortComparatorClass();
        Path infile = new Path("file:///D:\\Workspace\\amber\\hadoop\\data\\word.file");
        TextInputFormat.addInputPath(job, infile);
        Path outfile = new Path("file:///D:\\Workspace\\amber\\hadoop\\data\\output");
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        TextOutputFormat.setOutputPath(job, outfile);
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
