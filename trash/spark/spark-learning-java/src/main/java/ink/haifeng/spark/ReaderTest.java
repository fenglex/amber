package ink.haifeng.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * ReaderTest
 *
 * @author haifeng
 * @since 1.0
 */
public class ReaderTest {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("test");
        conf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("file:///Users/haifeng/workspace/Projects/amber/spark/spark-learning-java/src/main/java/ink/haifeng/spark/text.file", 1);

        long count = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    //list.add(iter.next()+",xxxxx");
                    iter.next();
                }
             /*   list.add("1");
                list.add("0");*/

                return list.iterator();
            }
        }).count();


        System.out.println(count);
        sc.stop();
    }
}
