package ink.haifeng.quotation.function;

import ink.haifeng.quotation.model.dto.BasicInfoData;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class TradeDayKeyedBroadcastProcessFunction
        extends KeyedBroadcastProcessFunction<Integer, StockData, BasicInfoData, StockData> {
    private ListState<StockData> stockDataCacheListState;


    private MapStateDescriptor<Integer, Boolean> basicInfoBroadcastStateDescriptor;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.basicInfoBroadcastStateDescriptor = new MapStateDescriptor<>(
                "trade_day_state", Types.INT, Types.BOOLEAN);
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

        Boolean isTradeDay =
                ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(ctx.getCurrentKey());
        // System.out.println("接收到数据:" + value);
        stockDataCacheListState.add(value);
        if (isTradeDay == null) {
            ctx.timerService().registerProcessingTimeTimer(ctx.currentProcessingTime() + 5 * 1000L);
        } else {
            if (isTradeDay) {
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
        ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).put(value.getTradeDay(),
                !value.getInfos().isEmpty());
    }

    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<Integer, StockData, BasicInfoData,
            StockData>.OnTimerContext ctx, Collector<StockData> out) throws Exception {

        Boolean isTradeDay =
                ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(ctx.getCurrentKey());
        if (isTradeDay != null) {
            if (isTradeDay) {
                for (StockData data : stockDataCacheListState.get()) {
                    out.collect(data);
                }
            }
            stockDataCacheListState.clear();
        }
    }
}
