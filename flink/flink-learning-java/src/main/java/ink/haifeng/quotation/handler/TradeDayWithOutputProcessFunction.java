package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.BasicInfoData;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 如果当日不是交易日，这过滤交易数据，如果是则往下游传输
 *
 * @author haifeng
 */
public class TradeDayWithOutputProcessFunction implements WithOutputHandler<SingleOutputStreamOperator<StockData>,
        SingleOutputStreamOperator<StockData>> {
    private BroadcastStream<BasicInfoData> broadcastStream;

    public TradeDayWithOutputProcessFunction(BroadcastStream<BasicInfoData> broadcastStream) {
        this.broadcastStream = broadcastStream;
    }

    @Override
    public SingleOutputStreamOperator<StockData> handler(SingleOutputStreamOperator<StockData> stream,
                                                         Properties properties) {
        return stream.keyBy(StockData::getTradeDay)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, StockData, BasicInfoData, StockData>() {
                    private ListState<StockData> stockDataCacheListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 存储未初始化前的数据
                        ListStateDescriptor<StockData> stockDataCache = new ListStateDescriptor<>(
                                "key_stock_data_cache",
                                Types.POJO(StockData.class));
                        stockDataCacheListState = getRuntimeContext().getListState(stockDataCache);
                    }

                    @Override
                    public void processElement(StockData value, KeyedBroadcastProcessFunction<Integer, StockData,
                            BasicInfoData, StockData>.ReadOnlyContext ctx, Collector<StockData> out) throws Exception {
                        MapStateDescriptor<Void, BasicInfoData> basicInfoBroadcastStateDescriptor =
                                new MapStateDescriptor<>(
                                        "basic_info_broadcast_state", Types.VOID, Types.POJO(BasicInfoData.class));
                        BasicInfoData basicInfo = ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(null);
                        // System.out.println("接收到数据:" + value);
                        stockDataCacheListState.add(value);
                        if (basicInfo == null || basicInfo.getTradeDay() < value.getTradeDay()) {
                            ctx.timerService().registerProcessingTimeTimer(ctx.currentProcessingTime() + 30 * 1000L);
                        }
                        if (basicInfo != null && basicInfo.getTradeDay() == value.getTradeDay() && !basicInfo.getInfos().isEmpty()) {
                            for (StockData data : stockDataCacheListState.get()) {
                                out.collect(data);
                            }
                            stockDataCacheListState.clear();
                        }
                    }

                    @Override
                    public void processBroadcastElement(BasicInfoData value, KeyedBroadcastProcessFunction<Integer,
                            StockData
                            , BasicInfoData, StockData>.Context ctx, Collector<StockData> out) throws Exception {
                        MapStateDescriptor<Void, BasicInfoData> basicInfoBroadcastStateDescriptor =
                                new MapStateDescriptor<>(
                                        "basic_info_broadcast_state", Types.VOID, Types.POJO(BasicInfoData.class));
                        ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).put(null, value);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<Integer, StockData, BasicInfoData,
                            StockData>.OnTimerContext ctx, Collector<StockData> out) throws Exception {
                        MapStateDescriptor<Void, BasicInfoData> basicInfoBroadcastStateDescriptor =
                                new MapStateDescriptor<>(
                                        "basic_info_broadcast_state", Types.VOID, Types.POJO(BasicInfoData.class));
                        BasicInfoData basicInfo = ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(null);
                        if (basicInfo != null) {
                            int tradeDay = basicInfo.getTradeDay();
                            boolean isTradeDay = !basicInfo.getInfos().isEmpty();
                            for (StockData data : stockDataCacheListState.get()) {
                                if (data.getTradeDay() == tradeDay && isTradeDay) {
                                    out.collect(data);
                                }
                            }
                            stockDataCacheListState.clear();
                        }
                    }
                });
    }
}
