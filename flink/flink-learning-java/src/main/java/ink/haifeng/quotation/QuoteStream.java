package ink.haifeng.quotation;

import ink.haifeng.quotation.function.DataFilterFunction;
import ink.haifeng.quotation.function.LastDataProcess;
import ink.haifeng.quotation.handler.StockDailyNoOutputHandler;
import ink.haifeng.quotation.handler.ToMinuteDataWithOutputHandler;
import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.source.ProductInfoSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

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
        args = new String[]{"-run.day", "20220418", "-redis.host", "192.168.2.8", "-redis.port", "6379", "-redis" +
                ".password", "123456", "-redis.database", "2", "-jdbc.url", "jdbc:mysql://192.168.2" + ".8:3306" +
                "/db_n_turbo_quotation?useUnicode=true&characterEncoding=UTF8" + "&autoReconnect=true", "-jdbc" +
                ".user", "root", "-jdbc.password", "123456", "-kafka.bootstrap", "192.168.2.8:9092", "-kafka" +
                ".topic", "0519", "-kafka.group", "quote"};
        ParameterTool param = ParameterTool.fromArgs(args);
        Properties properties = param.getProperties();
        env.getConfig().setGlobalJobParameters(param);


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers(param.get("kafka" +
                ".bootstrap")).setTopics(param.get("kafka.topic")).setGroupId(param.get("kafka.group")).setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
        SingleOutputStreamOperator<StockData> filterStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "kafka-source").map(StockData::new).returns(Types.POJO(StockData.class)).filter(new DataFilterFunction(param.get("run.day")));

        // 广播配置数据(获取每日的产品以及成分股信息)
        MapStateDescriptor<Void, ProductBroadcastData> basicInfoState = new MapStateDescriptor<>(
                "product_basic_info_state", Types.VOID, Types.POJO(ProductBroadcastData.class));
        BroadcastStream<ProductBroadcastData> broadcastStream =
                env.addSource(new ProductInfoSource()).broadcast(basicInfoState);
        // 结合广播数据判断当日是否是交易日

        filterStream.keyBy(StockData::getStockCode).connect(broadcastStream).process(new KeyedBroadcastProcessFunction<String, StockData, ProductBroadcastData, StockData>() {

            private ValueState<Integer> currentConfigState;
            private ValueState<Boolean> isTradeDayState;

            private ListState<StockData> stockDataCacheListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 当日是否已经初始化
                ValueStateDescriptor<Integer> currentDayIsConfig = new ValueStateDescriptor<>("current_day_is_config"
                        , Types.INT);
                currentConfigState = getRuntimeContext().getState(currentDayIsConfig);
                // 当日是否是交易日 （无basic_info 数据则不是交易日）
                ValueStateDescriptor<Boolean> currentIsTradeDay = new ValueStateDescriptor<>("current_is_trade_day",
                        Types.BOOLEAN);
                isTradeDayState = getRuntimeContext().getState(currentIsTradeDay);
                // 存储未初始化前的数据
                ListStateDescriptor<StockData> stockDataCache = new ListStateDescriptor<>("key_stock_data_cache"
                        , Types.POJO(StockData.class));
                stockDataCacheListState = getRuntimeContext().getListState(stockDataCache);
            }

            @Override
            public void processElement(StockData value, KeyedBroadcastProcessFunction<String, StockData,
                    ProductBroadcastData, StockData>.ReadOnlyContext ctx, Collector<StockData> out) throws Exception {

            }

            @Override
            public void processBroadcastElement(ProductBroadcastData value, KeyedBroadcastProcessFunction<String,
                    StockData, ProductBroadcastData, StockData>.Context ctx, Collector<StockData> out) throws Exception {
                int tradeDay = value.getTradeDay();
                ExecutionConfig.GlobalJobParameters parameters =
                        getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                
            }
        })

        // 生成1129, 1500数据 用于处理特殊时间
        SingleOutputStreamOperator<StockData> streamWithWaterMark =
                filterStream.keyBy(StockData::getStockCode).process(new LastDataProcess()).assignTimestampsAndWatermarks(WatermarkStrategy.<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(6)).withIdleness(Duration.ofSeconds(6)).withTimestampAssigner((SerializableTimestampAssigner<StockData>) (element, recordTimestamp) -> element.getTimestamp()));

        // 处理个股实时数据
//        StockRealTimeNoOutputHandler realTimeHandler = new StockRealTimeNoOutputHandler();
//        realTimeHandler.handler(streamWithWaterMark, properties);

        // 处理个股每分钟数据
        ToMinuteDataWithOutputHandler minuteDataWithOutputHandler = new ToMinuteDataWithOutputHandler();
        SingleOutputStreamOperator<StockMinuteWithPreData> minuteStockStream =
                minuteDataWithOutputHandler.handler(streamWithWaterMark, properties);
//        StockMinuteNoOutputHandler stockMinuteNoOutputHandler = new StockMinuteNoOutputHandler();
//        stockMinuteNoOutputHandler.handler(minuteStockStream, properties);

        // 个股每日数据
        StockDailyNoOutputHandler stockDailyNoOutputHandler = new StockDailyNoOutputHandler();
        stockDailyNoOutputHandler.handler(minuteStockStream, properties);
        // 广播配置数据
        MapStateDescriptor<Void, List<ProductInfo>> basicInfoState = new MapStateDescriptor<>(
                "product_basic_info_state", Types.VOID, Types.LIST(Types.POJO(ProductInfo.class)));

        BroadcastStream<List<ProductInfo>> broadcastStream =
                env.addSource(new ProductInfoSource()).broadcast(basicInfoState);

        minuteStockStream.keyBy(e -> e.getCurrent().getStockCode()).connect(broadcastStream).process(new KeyedBroadcastProcessFunction<String, StockMinuteWithPreData, List<ProductInfo>, ProductData>() {

            private ListState<StockMinuteWithPreData> dataCacheState;
            private ListState<ProductData> stockConstituentsSate;

            private ListStateDescriptor<ProductData> productStockConstituentsStateDescriptor;

            private ValueState<Integer> configDatState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("config_day",
                        Types.INT);
                configDatState = getRuntimeContext().getState(valueStateDescriptor);

                ListStateDescriptor<StockMinuteWithPreData> cacheMinuteDataStateDescriptor =
                        new ListStateDescriptor<>("cache_minute_data", Types.POJO(StockMinuteWithPreData.class));
                dataCacheState = getRuntimeContext().getListState(cacheMinuteDataStateDescriptor);

                ListStateDescriptor<ProductData> productStockConstituentsStateDescriptor = new ListStateDescriptor<>(
                        "stock_constituents_product", Types.POJO(ProductData.class));
                stockConstituentsSate = getRuntimeContext().getListState(productStockConstituentsStateDescriptor);
            }

            @Override
            public void processElement(StockMinuteWithPreData value, KeyedBroadcastProcessFunction<String,
                    StockMinuteWithPreData, List<ProductInfo>, ProductData>.ReadOnlyContext ctx,
                                       Collector<ProductData> out) throws Exception {
                StockData current = value.getCurrent();
                Integer configDay = configDatState.value();
                if (configDay == null || configDay != current.getTradeDay()) {
                    dataCacheState.add(value);
                }
            }

            @Override
            public void processBroadcastElement(List<ProductInfo> value, KeyedBroadcastProcessFunction<String,
                    StockMinuteWithPreData, List<ProductInfo>, ProductData>.Context ctx, Collector<ProductData> out) throws Exception {

                Integer tradeDay = null;
                stockConstituentsSate.clear();
                for (ProductInfo data : value) {
                    List<ProductStockConstituents> constituents = data.getConstituents();
                    ProductBasicInfo basicInfo = data.getBasicInfo();
                    ProductPriceLowHigh priceLowHigh = data.getPriceLowHigh();
                    tradeDay = basicInfo.getTradeDay();
                    for (ProductStockConstituents constituent : constituents) {
                        ctx.applyToKeyedState(productStockConstituentsStateDescriptor, new KeyedStateFunction<String,
                                ListState<ProductData>>() {
                            @Override
                            public void process(String key, ListState<ProductData> productDataListState) throws Exception {
                                if (constituent.getStockCode().equals(key)) {
                                    stockConstituentsSate.add(new ProductData(null, basicInfo, constituent,
                                            priceLowHigh));
                                }
                            }
                        });
                    }
                }
                configDatState.update(tradeDay);
            }
        });


        env.execute();
    }


}
