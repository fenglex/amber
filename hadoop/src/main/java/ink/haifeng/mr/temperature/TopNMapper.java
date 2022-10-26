package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopNMapper extends Mapper<Object, Text, TempInfo, IntWritable> {
    TempInfo tempInfo = new TempInfo();
    IntWritable value = new IntWritable();

    protected void map(Object key, Text value, Mapper<Object, Text, TempInfo, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().trim().split(",");
        tempInfo.setDay(Integer.parseInt(split[0]));
        tempInfo.setTemperature(Integer.parseInt(split[1]));
        this.value.set(Integer.parseInt(split[1]));
        context.write(tempInfo, this.value);
    }
}
