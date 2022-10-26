package ink.haifeng.mr.temperature;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopNMapper extends Mapper<LongWritable, Text, TempInfo, IntWritable> {


    TempInfo outKey = new TempInfo();
    IntWritable outValue = new IntWritable();
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TempInfo, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), ',');
        outKey.setDay(Integer.parseInt(split[0]));
        outKey.setTemperature(Integer.parseInt(split[1]));
        outValue.set(Integer.parseInt(split[1]));
        context.write(outKey, outValue);
    }
}
