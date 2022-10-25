package ink.haifeng.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class SortMapper extends Mapper<Object, Text, Text, Text> {
    // 该方法只会执行一次
    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    //hadoop框架中，它是一个分布式  数据 ：序列化、反序列化
    //hadoop有自己一套可以序列化、反序列化
    //或者自己开发类型必须：实现序列化，反序列化接口，实现比较器接口
    //排序 -》  比较  这个世界有2种顺序：  8  11，    字典序、数值顺序
    private final IntWritable count = new IntWritable(1);

    private final Text word = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        context.write(new Text(s[0]),new Text(s[1]));
        StringTokenizer itr = new StringTokenizer(value.toString());

    }
}
