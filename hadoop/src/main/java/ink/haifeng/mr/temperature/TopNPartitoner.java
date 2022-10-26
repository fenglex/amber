package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNPartitoner extends Partitioner<TempInfo, IntWritable> {
    @Override
    public int getPartition(TempInfo tempInfo, IntWritable intWritable, int numPartitions) {
        //1,不能太复杂。。。
        //partitioner  按  年，月  分区  -》
        // 分区 > 分组  按 年分区！！！！！！
        //分区器潜台词：满足  相同的key获得相同的分区号就可以~！
        return (tempInfo.getDay() / 10000) % numPartitions;
    }
}
