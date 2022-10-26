package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TopNReducer extends Reducer<TempInfo, IntWritable, Text, IntWritable> {

    Text key = new Text();
    IntWritable value = new IntWritable();

    @Override
    protected void reduce(TempInfo key, Iterable<IntWritable> values, Reducer<TempInfo, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int firstDay = -1;
        while (iterator.hasNext()) {
            IntWritable next = iterator.next();
            if (firstDay == -1) {
                this.key.set(String.valueOf(key.getDay()));
                this.value.set(key.getTemperature());
                context.write(this.key, this.value);
                firstDay = key.getDay();
            }
            if (firstDay != -1 && key.getDay() != firstDay) {
                this.key.set(String.valueOf(key.getDay()));
                this.value.set(key.getTemperature());
                context.write(this.key, this.value);
                break;
            }
        }
    }
}
