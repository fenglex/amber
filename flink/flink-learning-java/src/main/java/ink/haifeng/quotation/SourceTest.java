package ink.haifeng.quotation;

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

        KeyedStream<ProductIndexConstituents, String> constituentsStringKeyedStream =
                env.addSource(new ProductConstituentsSource()).returns(Types.POJO(ProductIndexConstituents.class))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<ProductIndexConstituents>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                        .withTimestampAssigner((SerializableTimestampAssigner<ProductIndexConstituents>) (element, recordTimestamp) -> element.timestamp())
                        ).keyBy(ProductIndexConstituents::getStockCode);


        DataStreamSource<String> streamSource = env
                .fromElements("000020.SZ,1650250112",
                        "000059.SZ,1650250112",
                        "000301.SZ,1650250112");
        streamSource.map((MapFunction<String,
                        Tuple2<String, String>>) value -> {
                    String[] split = value.split(",");
                    return Tuple2.of(split[0], split[1]);
                }).returns(Types.TUPLE(Types.STRING, Types.STRING))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, String>>) (element,
                                                                                                        recordTimestamp) -> Integer.parseInt(element.f1)*1000L)).keyBy(e -> e.f0)
                .intervalJoin(constituentsStringKeyedStream)
                .between(Time.hours(-8), Time.hours(0)
                ).process(new ProcessJoinFunction<Tuple2<String, String>, ProductIndexConstituents,
                        StringConstituent>() {
                    @Override
                    public void processElement(Tuple2<String, String> left, ProductIndexConstituents right,
                                               ProcessJoinFunction<Tuple2<String, String>, ProductIndexConstituents,
                                                       StringConstituent>.Context ctx,
                                               Collector<StringConstituent> out) throws Exception {
                        out.collect(new StringConstituent(left.f0 + "---" + left.f1, right));
                    }


                }).print("test");


        env.execute("timer test");
    }
}
