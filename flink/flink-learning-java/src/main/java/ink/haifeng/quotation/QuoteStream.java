package ink.haifeng.quotation;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.function.DataFilterFunction;
import ink.haifeng.quotation.function.LastDataProcess;
import ink.haifeng.quotation.function.TradeDayKeyedBroadcastProcessFunction;
import ink.haifeng.quotation.handler.StockMinuteNoOutputHandler;
import ink.haifeng.quotation.handler.ToMinuteDataWithOutputHandler;
import ink.haifeng.quotation.model.dto.BasicInfoData;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockMinuteData;
import ink.haifeng.quotation.source.ProductInfoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        //env.setStateBackend(new FsStateBackend());
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

        SingleOutputStreamOperator<StockData> filterStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(StockData::new)
                .returns(Types.POJO(StockData.class))
                .filter(new DataFilterFunction(param.get(Constants.RUN_DAY)));


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
        filterStream = filterStream.keyBy(StockData::getTradeDay)
                .connect(broadcastStream)
                .process(new TradeDayKeyedBroadcastProcessFunction());

        // 生成1129, 1500数据 用于处理特殊时间
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
        ToMinuteDataWithOutputHandler minuteDataWithOutputHandler = new ToMinuteDataWithOutputHandler();

        SingleOutputStreamOperator<StockMinuteData> minuteStockStream =
                minuteDataWithOutputHandler.handler(streamWithWaterMark, properties);

        //minuteStockStream.print("minute-data");


        StockMinuteNoOutputHandler stockMinuteNoOutputHandler = new StockMinuteNoOutputHandler(broadcastStream);
        stockMinuteNoOutputHandler.handler(minuteStockStream, properties);


        // minuteStockStream.print("stock-minute");

        // 个股每日数据
  /*      StockDailyNoOutputHandler stockDailyNoOutputHandler = new StockDailyNoOutputHandler();
        stockDailyNoOutputHandler.handler(minuteStockStream, properties);*/

        // ProductMinuteNoOutPutHandler handler = new ProductMinuteNoOutPutHandler(broadcastStream);
        // handler.handler(minuteStockStream, properties);


        env.execute();
    }


}
