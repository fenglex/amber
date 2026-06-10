package ink.haifeng.quotation;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.function.*;
import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.model.entity.ProductEod;
import ink.haifeng.quotation.model.entity.StockDaily;
import ink.haifeng.quotation.model.entity.StockQuotation;
import ink.haifeng.quotation.sink.ProductEodSink;
import ink.haifeng.quotation.sink.ProductQuotationSink;
import ink.haifeng.quotation.sink.StockDailySink;
import ink.haifeng.quotation.sink.StockQuotationSink;
import ink.haifeng.quotation.source.ProductInfoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/22 13:37:25
 */


public class QuoteStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getConfig().setAutoWatermarkInterval(200);
        //env.enableCheckpointing(2000L);
        args = new String[]{"-run.day", "20220418", "-redis.host", "192.168.2.8", "-redis.port", "6379", "-redis" +
                ".password", "123456", "-redis.database", "2", "-jdbc.url", "jdbc:mysql://127.0.0.1:3306" +
                "/db_n_turbo_quotation?useUnicode=true&characterEncoding=UTF8" + "&autoReconnect=true", "-jdbc" +
                ".user", "root", "-jdbc.password", "123456", "-kafka.bootstrap", "192.168.2.8:9092", "-kafka" +
                ".topic", "0418_all", "-kafka.group", "quote2"};
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

        SingleOutputStreamOperator<StockData> kafkaStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(StockData::new)
                .returns(Types.POJO(StockData.class));
        // 当日basic—info数据

        // filterStream.print("filter-stream");

        DataStreamSource<BasicInfoData> basicInfoStream = env.addSource(new ProductInfoSource()).setParallelism(1);

        MapStateDescriptor<Integer, Boolean> basicInfoBroadcastStateDescriptor = new MapStateDescriptor<>(
                "trade_day_state", Types.INT, Types.BOOLEAN);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(6))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        basicInfoBroadcastStateDescriptor.enableTimeToLive(ttlConfig);
        BroadcastStream<BasicInfoData> tradeDayBroadcastStream =
                basicInfoStream.broadcast(basicInfoBroadcastStateDescriptor);


        MapStateDescriptor<Integer, BasicInfoData> basicInfoAll = new MapStateDescriptor<>(
                "basic_info_broadcast_state", Types.INT,
                Types.POJO(BasicInfoData.class));
        BroadcastStream<BasicInfoData> broadcastStream = basicInfoStream.broadcast(basicInfoAll);

        // 过滤非交易日数据
        SingleOutputStreamOperator<StockData> filterStream = filterStream(kafkaStream,
                tradeDayBroadcastStream, properties);

        // 生成水印  生成1129, 1500数据 用于处理特殊时间
        SingleOutputStreamOperator<StockData> streamWithWaterMark =
                filterStream.keyBy(StockData::getStockCode)
                        .process(new LastDataProcess())
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(7))
                                .withIdleness(Duration.ofSeconds(7))
                                .withTimestampAssigner((SerializableTimestampAssigner<StockData>) (element,
                                                                                                   recordTimestamp)
                                        -> element
                                        .getTimestamp())).filter(e -> e.getState() > 0);

        // 处理个股实时数据
//        StockRealTimeNoOutputHandler realTimeHandler = new StockRealTimeNoOutputHandler();
//        realTimeHandler.handler(streamWithWaterMark, properties);

        // 处理个股每分钟数据
        SingleOutputStreamOperator<StockMinuteData> minuteStockDataStream =
                streamWithWaterMark.keyBy(StockData::getStockCode)
                        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                        .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
                        .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value2.getRealtime()) >= Long.parseLong(value1.getRealtime()) ? value2 : value1)
                        .keyBy(StockData::getTradeDay)
                        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                        .aggregate(new StockMinuteDataAggregateFunction());


        SingleOutputStreamOperator<StockQuotation> stockMinuteQuotationStream =
                minuteStockDataStream.keyBy(StockMinuteData::getTradeDay)
                        .process(new StockMinuteKeyedProcessFunction());

        processQuotation(stockMinuteQuotationStream,properties,broadcastStream);
        env.execute();
    }


    private static SingleOutputStreamOperator<StockData> filterStream(SingleOutputStreamOperator<StockData> kafkaStream,
                                                                      BroadcastStream<BasicInfoData> tradeDayBroadcastStream,
                                                                      Properties properties) {
        return kafkaStream.filter(new DataFilterFunction(properties.getProperty(Constants.RUN_DAY)))
                .keyBy(StockData::getTradeDay)
                .connect(tradeDayBroadcastStream)
                .process(new TradeDayKeyedBroadcastProcessFunction());
    }

    private static void processQuotation(SingleOutputStreamOperator<StockQuotation> stockMinuteQuotationStream,
                                         Properties properties, BroadcastStream<BasicInfoData> broadcastStream) {
        // 个股每分钟
        StockQuotationSink stockQuotationSink = new StockQuotationSink(properties);
        //stockMinuteQuotationStream.addSink(stockQuotationSink.sink());
        stockMinuteQuotationStream.print("stock-minute");

        // 个股每日

        OutputTag<StockData> stockEodTag = new OutputTag<StockData>("stock-eod-output") {
        };
        StockDailySink stockDailySink = new StockDailySink(properties);
        //stockMinuteQuotationStream.getSideOutput(stockEodTag).map(StockDaily::new).addSink(stockDailySink.sink());
        stockMinuteQuotationStream.getSideOutput(stockEodTag).map(StockDaily::new).print("stock-eod");

        // 每分钟产品

        SingleOutputStreamOperator<ProductQuotation> productMinuteStream =
                stockMinuteQuotationStream.keyBy(StockQuotation::getTradeDay)
                        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                        .aggregate(new StockMinuteQuotationAggregateFunction())
                        .keyBy(MinuteStockQuotation::getTradeDay)
                        .connect(broadcastStream)
                        .process(new ProductMinuteKeyedBroadcastProcessFunction());

        ProductQuotationSink productQuotationSink = new ProductQuotationSink(properties);
        // productMinuteStream.addSink(productQuotationSink.sink());
        productMinuteStream.print("product-minute");

        // 每日产品
        OutputTag<ProductEod> productEodOutputTag = new OutputTag<ProductEod>("product-eod-output") {
        };

        ProductEodSink productEodSink = new ProductEodSink(properties);
        //productMinuteStream.getSideOutput(productEodOutputTag).addSink(productEodSink.sink());

        productMinuteStream.getSideOutput(productEodOutputTag).print("product-eod");
    }

}
