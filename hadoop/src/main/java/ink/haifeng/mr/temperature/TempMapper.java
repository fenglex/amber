package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @author haifeng
 */
public class TempMapper extends Mapper<Object, Text, Text, IntWritable> {


    final Text text = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {


        text.set(value);
        context.write(text, null);
    }
}
