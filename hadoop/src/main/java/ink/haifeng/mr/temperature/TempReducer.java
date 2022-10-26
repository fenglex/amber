package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author : haifeng
 */
public class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private String firstDay;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable next = iterator.next();
            String[] split = key.toString().split(",");
            if (firstDay == null) {
                context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])));
                firstDay = split[0];
            } else if (!split[0].equals(firstDay)) {
                context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])));
            }
        }

    }
}
