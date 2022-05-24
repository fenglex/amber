package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.BasicInfoData;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
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


                    private MapStateDescriptor<Integer, BasicInfoData> basicInfoBroadcastStateDescriptor;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.basicInfoBroadcastStateDescriptor = new MapStateDescriptor<>(
                                "basic_info_broadcast_state", Types.INT, Types.POJO(BasicInfoData.class));
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(6))
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        this.basicInfoBroadcastStateDescriptor.enableTimeToLive(ttlConfig);
                        // 存储未初始化前的数据
                        ListStateDescriptor<StockData> stockDataCache = new ListStateDescriptor<>(
                                "key_stock_data_cache",
                                Types.POJO(StockData.class));
                        stockDataCache.enableTimeToLive(ttlConfig);
                        stockDataCacheListState = getRuntimeContext().getListState(stockDataCache);
                    }

                    @Override
                    public void processElement(StockData value, KeyedBroadcastProcessFunction<Integer, StockData,
                            BasicInfoData, StockData>.ReadOnlyContext ctx, Collector<StockData> out) throws Exception {
                        BasicInfoData basicInfo =
                                ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(ctx.getCurrentKey());
                        // System.out.println("接收到数据:" + value);
                        stockDataCacheListState.add(value);
                        if (basicInfo == null) {
                            ctx.timerService().registerProcessingTimeTimer(ctx.currentProcessingTime() + 30 * 1000L);
                        } else {
                            if (!basicInfo.getInfos().isEmpty()) {
                                for (StockData data : stockDataCacheListState.get()) {
                                    out.collect(data);
                                }
                            }
                            stockDataCacheListState.clear();
                        }
                    }

                    @Override
                    public void processBroadcastElement(BasicInfoData value, KeyedBroadcastProcessFunction<Integer,
                            StockData
                            , BasicInfoData, StockData>.Context ctx, Collector<StockData> out) throws Exception {
                        ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).put(value.getTradeDay(), value);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<Integer, StockData, BasicInfoData,
                            StockData>.OnTimerContext ctx, Collector<StockData> out) throws Exception {
                        BasicInfoData basicInfo =
                                ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(ctx.getCurrentKey());
                        if (basicInfo != null) {
                            boolean isTradeDay = !basicInfo.getInfos().isEmpty();
                            for (StockData data : stockDataCacheListState.get()) {
                                if (isTradeDay) {
                                    out.collect(data);
                                }
                            }
                            stockDataCacheListState.clear();
                        }
                    }
                });
    }
}
