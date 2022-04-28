package ink.haifeng.quotation;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.common.TimeUtil;
import ink.haifeng.quotation.function.DataFilterFunction;
import ink.haifeng.quotation.function.LastDataProcess;
import ink.haifeng.quotation.model.dto.RedisValue;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.sink.RedisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/22 13:37:25
 */


public class QuoteStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setAutoWatermarkInterval(200);
        args = new String[]{"-run.day", "20220418",
                "-redis.host", "192.168.1.2", "-redis.port", "6379", "-redis.password", "123456",
                "-redis.database", "2"};
        ParameterTool param = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(param);
        SingleOutputStreamOperator<StockData> filterStream = env
                .fromSource(kafkaSource("192.168.1.2:9092", "original_0418", "quote"),
                        WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(StockData::new)
                .returns(Types.POJO(StockData.class))
                .filter(new DataFilterFunction(param.get("run.day")));

        SingleOutputStreamOperator<StockData> streamWithWaterMark =
                filterStream.keyBy(StockData::getStockCode).process(new LastDataProcess()).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                        .withIdleness(Duration.ofSeconds(6))
                        .withTimestampAssigner((SerializableTimestampAssigner<StockData>)
                                (element, recordTimestamp) -> element.getTimestamp()));

        streamWithWaterMark.keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long.parseLong(value2.getRealtime()) ?
                        value1 : value2)
                .filter(e -> TimeUtil.isTradeTime(e.minute()))
                .map((MapFunction<StockData, RedisValue>) value -> new RedisValue(Constants.STOCK_REDIS_KEY,
                        value.getCodePrefix(), value.redisString()))
                .addSink(new RedisSink());

        streamWithWaterMark
                .keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).allowedLateness(Time.seconds(7))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long.parseLong(value2.getRealtime()) ?
                        value1 : value2);


//        SingleOutputStreamOperator<StockQuotation> minuteStream = filterStream.keyBy(StockData::getStockCode)
//                .flatMap(new LastDataFlatMap()).assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(4))
//                        .withIdleness(Duration.ofSeconds(3))
//                        .withTimestampAssigner((SerializableTimestampAssigner<StockData>) (element,
//                        recordTimestamp) -> element.getTimestamp()))
//                .keyBy(StockData::getStockCode)
//                .window(TumblingEventTimeWindows.of(Time.minutes(1))).allowedLateness(Time.seconds(4))
//                .reduce((ReduceFunction<StockData>) (v1, v2) -> v1.getTimestamp() > v2.getTimestamp() ? v1 : v2)
//                .keyBy(StockData::getStockCode)
//                .map(new RichMapFunction<StockData, StockQuotation>() {
//                    private ValueState<StockData> lastMinuteValueState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        ValueStateDescriptor<StockData> descriptor =
//                                new ValueStateDescriptor<>("last-minute-stock-data",
//                                        Types.POJO(StockData.class));
//                        lastMinuteValueState = this.getRuntimeContext().getState(descriptor);
//                    }
//
//                    @Override
//                    public StockQuotation map(StockData value) throws Exception {
//                        StockData stateValue = lastMinuteValueState.value();
//                        lastMinuteValueState.update(value);
//                        return new StockQuotation(value, stateValue);
//                    }
//                });
//
//        minuteStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(60)));

        env.execute();
    }


    private static KafkaSource<String> kafkaSource(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

}
