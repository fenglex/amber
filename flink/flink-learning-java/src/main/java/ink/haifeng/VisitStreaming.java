package ink.haifeng;

import ink.haifeng.dto.VisitLog;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/3/28 18:18:14
 */
public class VisitStreaming {
    public static void main(String[] args) throws Exception {
        ExecutionConfig config = new ExecutionConfig();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<VisitLog> result = source.map(e -> {
                    String[] split = e.split(",");
                    return new VisitLog(split[0], split[1]);
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<VisitLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, timestamp) -> e.getTimestamp() * 1000L))
                .keyBy(VisitLog::getRegion)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .reduce((v1, v2) -> v1.getTimestamp() > v2.getTimestamp() ? v1 : v2);

        result.print("result");
        env.execute("test job");
    }
}
