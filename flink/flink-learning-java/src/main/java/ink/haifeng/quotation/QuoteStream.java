package ink.haifeng.quotation;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.common.TimeUtil;
import ink.haifeng.quotation.function.DataFilterFunction;
import ink.haifeng.quotation.function.LastDataProcess;
import ink.haifeng.quotation.model.dto.RedisValue;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.entity.StockQuotation;
import ink.haifeng.quotation.sink.RedisSink;
import ink.haifeng.quotation.sink.RedisValueSink;
import ink.haifeng.quotation.sink.SinkFactory;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
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
                "-redis.database", "2",
                "-jdbc.url",
                "jdbc:mysql://192.168.1.2:3306/db_n_turbo_quotation?useUnicode=true&characterEncoding=UTF8" +
                        "&autoReconnect=true"
                , "-jdbc.user", "root", "-jdbc.password", "123456"};
        ParameterTool param = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(param);
        SingleOutputStreamOperator<StockData> filterStream = env
                .fromSource(kafkaSource("192.168.1.2:9092", "original_0418", "quote"),
                        WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(StockData::new)
                .returns(Types.POJO(StockData.class))
                .filter(new DataFilterFunction(param.get("run.day")));

        SinkFactory sinkFactory = new SinkFactory(param);
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
                .addSink(sinkFactory.<RedisValueSink>getSink(RedisValueSink.class));


        SingleOutputStreamOperator<StockQuotation> minuteStream = streamWithWaterMark
                .keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).allowedLateness(Time.seconds(7))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long.parseLong(value2.getRealtime()) ?
                        value1 : value2)
                .keyBy(StockData::getStockCode).map(new RichMapFunction<StockData, StockQuotation>() {
                    private ValueState<StockData> stockLastMinuteState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stockLastMinuteState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<StockData>("last-stock-minute",
                                        Types.POJO(StockData.class)));
                    }

                    @Override
                    public StockQuotation map(StockData value) throws Exception {
                        StockData last = stockLastMinuteState.value();
                        if (last != null && last.getTradeDay() == value.getTradeDay()) {
                            System.out.println(String.format("%s\t%s\t%s\t%s", value.getStockCode(), value.minute(),
                                    last.getStockCode(), last.minute()));
                            return new StockQuotation(value, last);
                        } else {
                            return new StockQuotation(value, null);
                        }
                    }
                }).filter(e -> TimeUtil.isTradeTime(e.getTradeTime()));


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
