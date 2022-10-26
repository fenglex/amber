package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author : haifeng
 */
public class TempPartitioner extends Partitioner<Text, IntWritable> {
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String[] split = text.toString().split(",");
        return split[0].hashCode() % numPartitions;
    }
}
