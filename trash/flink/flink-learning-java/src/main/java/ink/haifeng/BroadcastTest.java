package ink.haifeng;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/23 16:10:33
 */
public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < 100; i++) {
                    ctx.collect("u" + i);
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {

            }
        });

        DataStreamSource<String> socketTextStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < 100; i++) {
                    Thread.sleep(1000);
                    ctx.collect("uu" + i);
                }
            }

            @Override
            public void cancel() {

            }
        });

        MapStateDescriptor<Void, String> stateDescriptor = new MapStateDescriptor<>("broadcast", Types.VOID,
                Types.STRING);
        BroadcastStream<String> broadcastStream = socketTextStream.broadcast(stateDescriptor);
        streamSource.keyBy(e -> e).connect(broadcastStream).process(new KeyedBroadcastProcessFunction<String, String,
                String, String>() {
            @Override
            public void processElement(String value,
                                       KeyedBroadcastProcessFunction<String, String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                String bsValue = ctx.getBroadcastState(stateDescriptor).get(null);
                out.collect(value + "___" + bsValue);
            }

            @Override
            public void processBroadcastElement(String value, KeyedBroadcastProcessFunction<String, String, String,
                    String>.Context ctx, Collector<String> out) throws Exception {
                ctx.getBroadcastState(stateDescriptor).put(null, value);
            }
        }).print("bs_test");


        env.execute();
    }
}
