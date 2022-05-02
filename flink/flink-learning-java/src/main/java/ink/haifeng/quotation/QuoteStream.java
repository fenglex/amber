package ink.haifeng.quotation;

import ink.haifeng.dto.ProductData;
import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.common.DateUtils;
import ink.haifeng.quotation.function.DataFilterFunction;
import ink.haifeng.quotation.function.LastDataProcess;
import ink.haifeng.quotation.function.ProcessProductMinuteProcessFunction;
import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.model.entity.ProductIndexConstituents;
import ink.haifeng.quotation.sink.RedisValueSink;
import ink.haifeng.quotation.sink.SinkFactory;
import ink.haifeng.quotation.source.ProductConstituentsSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

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
        env.getConfig().setAutoWatermarkInterval(200);
        args = new String[]{"-run.day", "20220418", "-redis.host", "192.168.2.6", "-redis.port", "6379",
                "-redis.password", "123456", "-redis.database", "2", "-jdbc.url",
                "jdbc:mysql://192.168.2.6:3306/db_n_turbo_quotation?useUnicode=true&characterEncoding=UTF8" +
                        "&autoReconnect=true",
                "-jdbc.user", "root", "-jdbc.password", "123456"};
        ParameterTool param = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(param);
        SingleOutputStreamOperator<StockData> filterStream = env
                .fromSource(kafkaSource("192.168.2.6:9092", "original_0418", "quote"),
                        WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(StockData::new)
                .returns(Types.POJO(StockData.class))
                .filter(new DataFilterFunction(param.get("run.day")));

        // 生成1129, 1500数据
        SingleOutputStreamOperator<StockData> streamWithWaterMark =
                filterStream
                        .keyBy(StockData::getStockCode)
                        .process(new LastDataProcess())
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                                .withIdleness(Duration.ofSeconds(6))
                                .withTimestampAssigner((SerializableTimestampAssigner<StockData>)
                                        (element, recordTimestamp) -> element.getTimestamp()))
                        .filter(e -> e.getState() != -2);

        RedisValueSink sink = SinkFactory.getSink(RedisValueSink.class, param);


        // TODO 生成方法
        // 生成实时个股数据（简化为30秒）
        streamWithWaterMark.keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long.parseLong(value2.getRealtime()) ?
                        value1 : value2)
                .filter(e -> DateUtils.isTradeTime(e.minute()))
                .map((MapFunction<StockData, RedisValue>) value -> new RedisValue(Constants.STOCK_REDIS_KEY,
                        value.getCodePrefix(), value.redisString()));
        //.addSink(sink.sink());


        //个股分钟数据(包含前一分钟数据)
        SingleOutputStreamOperator<StockMinutePreData> stockMinuteStream = streamWithWaterMark
                .keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).allowedLateness(Time.seconds(7))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long.parseLong(value2.getRealtime()) ?
                        value1 : value2)
                .keyBy(StockData::getStockCode).map(new RichMapFunction<StockData, StockMinutePreData>() {
                    private ValueState<StockData> stockLastMinuteState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stockLastMinuteState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<StockData>("last-stock-minute",
                                        Types.POJO(StockData.class)));
                    }

                    @Override
                    public StockMinutePreData map(StockData value) throws Exception {
                        StockData last = stockLastMinuteState.value();
                        StockMinutePreData data;
                        if (last != null && last.getTradeDay() == value.getTradeDay()) {
//                            System.out.println(String.format("%s\t%s\t%s\t%s", value.getStockCode(), value.minute(),
//                                    last.getStockCode(), last.minute()));
                            data = new StockMinutePreData(value, last);
                        } else {
                            data = new StockMinutePreData(value, null);
                        }
                        stockLastMinuteState.update(value);
                        return data;

                    }
                }).returns(Types.POJO(StockMinutePreData.class));


        MapStateDescriptor<String, ProductIndexConstituents> constituentsMapStateDescriptor =
                new MapStateDescriptor<>("product_constituents", Types.STRING,
                        Types.POJO(ProductIndexConstituents.class));
        BroadcastStream<List<ProductConstituentsInfo>> constituentsBroadcastStream = env.addSource(new ProductConstituentsSource()).broadcast(constituentsMapStateDescriptor);


        stockMinuteStream
                .keyBy(e -> e.getCurrent().getStockCode())
                .connect(constituentsBroadcastStream)
                .process(new ProcessProductMinuteProcessFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductData>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((SerializableTimestampAssigner<ProductData>) (stockMinutePreData, l) -> stockMinutePreData.getLastMinute().getTimestamp()))
                .print();
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<ProductData>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                        .withTimestampAssigner((SerializableTimestampAssigner<ProductData>) (productData, l) -> productData.getCurrent().getTimestamp()))
//                .keyBy(e -> e.getConstituentsInfo().getProductCode())
//                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//                .aggregate(new AggregateFunction<ProductData, ProductAccumulator, ProductAccumulator>() {
//
//
//                    @Override
//                    public ProductAccumulator createAccumulator() {
//                        return new ProductAccumulator();
//                    }
//
//                    @Override
//                    public ProductAccumulator add(ProductData data, ProductAccumulator productAccumulator) {
//                        StockData current = data.getCurrent();
//                        StockData lastMinute = data.getLastMinute();
//                        ProductConstituentsInfo info = data.getConstituentsInfo();
//                        productAccumulator.setProductCode(info.getProductCode());
//                        productAccumulator.setProductName(info.getProductName());
//                        productAccumulator.setTradeDay(current.getTradeDay());
//                        productAccumulator.setTradeTime(current.minute());
//                        if (lastMinute != null) {
//                            productAccumulator.setAmount(current.getAmount() - lastMinute.getAmount());
//                        }
//
//                        //System.out.println(productAccumulator.getUuid() + "\t" + data);
//
//                        return productAccumulator;
//                    }
//
//                    @Override
//                    public ProductAccumulator getResult(ProductAccumulator productAccumulator) {
//                        return productAccumulator;
//                    }
//
//                    @Override
//                    public ProductAccumulator merge(ProductAccumulator productAccumulator, ProductAccumulator acc1) {
//                        return null;
//                    }
//                });
        //.print("test");

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
