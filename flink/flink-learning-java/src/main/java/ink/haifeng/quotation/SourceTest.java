package ink.haifeng.quotation;

import ink.haifeng.quotation.model.dto.ProductConstituentsInfo;
import ink.haifeng.quotation.model.entity.ProductIndexConstituents;
import ink.haifeng.quotation.model.dto.StringConstituent;
import ink.haifeng.quotation.source.ProductConstituentsSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.api.scala.typeutils.Types.STRING;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 13:57:12
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        args = new String[]{"-run_day", "20220107",
                "-quotation-url", "jdbc:mysql://192.168.1.2:3306/db_n_turbo_quotation?useUnicode=true" +
                "&characterEncoding=utf8",
                "-quotation-user", "root", "-quotation-pass", "123456"};
        ParameterTool param = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(param);

        MapStateDescriptor<String, ProductIndexConstituents> constituentsStateDescriptor =
                new MapStateDescriptor<>("product-info", STRING(),
                        Types.POJO(ProductIndexConstituents.class));




        env.execute("timer test");
    }
}
