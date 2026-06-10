package ink.haifeng;


import cn.hutool.core.thread.ThreadUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class KeyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888);

        TestSource source = new TestSource();

        MapStateDescriptor<Void, String> stateDescriptor = new MapStateDescriptor<>("bs", Types.VOID, Types.STRING);

        BroadcastStream<String> broadcast = stream.broadcast(stateDescriptor);

        env.addSource(source).keyBy(e -> e.split("_")[0]).connect(broadcast).process(new KeyedBroadcastProcessFunction<String, String, String, String>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state",
                        Types.STRING);
                valueState = this.getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(String value,
                                       KeyedBroadcastProcessFunction<String, String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<Void, String> state = ctx.getBroadcastState(stateDescriptor);
                out.collect(value + "_" + state.get(null));
                valueState.update(state.get(null));
            }

            @Override
            public void processBroadcastElement(String value, KeyedBroadcastProcessFunction<String, String, String,
                    String>.Context ctx, Collector<String> out) throws Exception {
                ctx.getBroadcastState(stateDescriptor).put(null, value);
            }
        }).keyBy(e -> e.split("_")[0]).process(new KeyedProcessFunction<String, String, String>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state",
                        Types.STRING);
                valueState = this.getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(String value, KeyedProcessFunction<String, String, String>.Context ctx,
                                       Collector<String> out) throws Exception {
                out.collect(value + "_" + valueState.value());
            }
        }).print("test");

        env.execute();
    }


    private static class TestSource implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < 100; i++) {
                ctx.collect("user_" + i);
                ThreadUtil.sleep(1, TimeUnit.SECONDS);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
