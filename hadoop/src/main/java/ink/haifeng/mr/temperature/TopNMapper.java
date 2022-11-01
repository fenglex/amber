package ink.haifeng.mr.temperature;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class TopNMapper extends Mapper<LongWritable, Text, TempInfo, IntWritable> {


    /**
     * 可以在这里读取mysql或者
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, TempInfo, IntWritable>.Context context) throws IOException, InterruptedException {

        // 获取参数
        // Configuration configuration = context.getConfiguration();
        // 读取缓存文件
//        URI[] files = context.getCacheFiles();
//        Path path = new Path(files[0].getPath());
//        BufferedReader reader = new BufferedReader(new FileReader(new File(path.getName())));
//        String line = reader.readLine();
//        while(line != null){
//            String[] split = line.split("\t");
//            dict.put(split[0],split[1]);
//            line = reader.readLine();
//        }

    }

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
