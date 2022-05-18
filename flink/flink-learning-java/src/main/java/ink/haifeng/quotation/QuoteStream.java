package ink.haifeng.quotation;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.common.DateUtils;
import ink.haifeng.quotation.function.DataFilterFunction;
import ink.haifeng.quotation.function.LastDataProcess;
import ink.haifeng.quotation.model.dto.ProductDimensionData;
import ink.haifeng.quotation.model.dto.RedisValue;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockMinuteWithPreData;
import ink.haifeng.quotation.model.entity.StockQuotation;
import ink.haifeng.quotation.sink.RedisValueSink;
import ink.haifeng.quotation.sink.SinkFactory;
import ink.haifeng.quotation.sink.StockQuotationSink;
import ink.haifeng.quotation.source.ProductInfoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

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
        args = new String[]{"-run.day", "20220418", "-redis.host", "192.168.2.8", "-redis.port", "6379",
                "-redis.password", "123456", "-redis.database", "2", "-jdbc.url",
                "jdbc:mysql://192.168.2.8:3306/db_n_turbo_quotation?useUnicode=true&characterEncoding=UTF8" +
                        "&autoReconnect=true",
                "-jdbc.user", "root", "-jdbc.password", "123456",
                "-kafka.bootstrap", "192.168.2.8:9092", "-kafka.topic", "original_0418_2",
                "-kafka.group", "quote"};
        ParameterTool param = ParameterTool.fromArgs(args);
        Properties properties = param.getProperties();
        env.getConfig().setGlobalJobParameters(param);


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(param.get("kafka.bootstrap"))
                .setTopics(param.get("kafka.topic"))
                .setGroupId(param.get("kafka.group"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        SingleOutputStreamOperator<StockData> filterStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(StockData::new)
                .returns(Types.POJO(StockData.class))
                .filter(new DataFilterFunction(param.get("run.day")));

        // 生成1129, 1500数据 用于处理特殊时间
        SingleOutputStreamOperator<StockData> streamWithWaterMark =
                filterStream
                        .keyBy(StockData::getStockCode)
                        .process(new LastDataProcess())
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                                .withIdleness(Duration.ofSeconds(6))
                                .withTimestampAssigner((SerializableTimestampAssigner<StockData>)
                                        (element, recordTimestamp) -> element.getTimestamp()))
                        .filter(e -> e.getState() != -1);


        RedisValueSink redisValueSink = SinkFactory.getSink(RedisValueSink.class, param);
        // TODO 生成方法
        // 生成实时个股数据（简化为30秒）
/*
        streamWithWaterMark.keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long
                .parseLong(value2.getRealtime()) ?
                        value1 : value2)
                .filter(e -> DateUtils.isTradeTime(e.minute()))
                .map((MapFunction<StockData, RedisValue>) value -> new RedisValue(Constants.STOCK_REDIS_KEY,
                        value.getCodePrefix(), value.redisString()))
                .addSink(redisValueSink.sink());
*/

        // 个股分钟数据
        SingleOutputStreamOperator<StockMinuteWithPreData> stockMinuteData =
                streamWithWaterMark.filter(e -> e.getStockCode().equals("600519.SH")).keyBy(StockData::getStockCode)
                        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                        .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long.parseLong(value2.getRealtime()) ?
                                value1 : value2)
                        .keyBy(StockData::getStockCode).map(new RichMapFunction<StockData, StockMinuteWithPreData>() {
                            private ValueState<StockData> stockLastMinuteState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                stockLastMinuteState = getRuntimeContext()
                                        .getState(new ValueStateDescriptor<StockData>("last-stock-minute",
                                                Types.POJO(StockData.class)));
                            }

                            @Override
                            public StockMinuteWithPreData map(StockData value) throws Exception {
                                StockData last = stockLastMinuteState.value();
                                StockMinuteWithPreData data;
                                if (last != null && last.getTradeDay() == value.getTradeDay()) {
                                    System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s", value.getStockCode(),
                                            value.minute(), value.getRealtime(),
                                            last.getStockCode(), last.minute(), last.getRealtime()));
                                    data = new StockMinuteWithPreData(value, last);
                                } else {
                                    data = new StockMinuteWithPreData(value, null);
                                }
                                stockLastMinuteState.update(value);
                                return data;

                            }
                        }).returns(Types.POJO(StockMinuteWithPreData.class));
        StockQuotationSink stockQuotationSink = new StockQuotationSink(param);
        stockMinuteData.map(e -> new StockQuotation(e.getCurrent(), e.getLastMinute()))
                .addSink(stockQuotationSink.sink());
        stockMinuteData.print();

     /*   MapStateDescriptor<Void, List<ProductDimensionData>> mapStateDescriptor = new MapStateDescriptor<>(
                "product_constituents", Types.VOID,
                Types.LIST(Types.POJO(ProductDimensionData.class)));
        ProductInfoSource productInfoSource = new ProductInfoSource();
        BroadcastStream<List<ProductDimensionData>> basicInfoBroadcast =
                env.addSource(productInfoSource).broadcast(mapStateDescriptor);

        stockMinuteData.keyBy(e->e.getCurrent().getStockCode());*/
        // 产品分钟数据

        stockMinuteData.print("stock-minute");
        env.execute();
    }


}
