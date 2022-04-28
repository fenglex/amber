package ink.haifeng.quotation;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 01:23:23
 */
public class SocketTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost",7777)
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> {
                    String[] split = value.split(",");
                    return Tuple2.of(split[0],Integer.parseInt(split[1]));
                }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(e->e.f0)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Integer>>) (element,
                                                                                                         recordTimestamp) -> element.f1*1000L))
                .keyBy(e->e.f0).window(TumblingEventTimeWindows.of(Time.seconds(2))).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
                                                          Tuple2<String, Integer> value2) throws Exception {
                        return value1.f1>value2.f1?value1:value2;
                    }
                }).print("Test");
        env.execute("stock-test-job");
    }
}
