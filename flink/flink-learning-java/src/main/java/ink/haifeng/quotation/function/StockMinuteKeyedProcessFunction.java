package ink.haifeng.quotation.function;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.model.entity.ProductEod;
import ink.haifeng.quotation.model.entity.StockQuotation;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/24 15:21:21
 */
public class StockMinuteKeyedProcessFunction extends KeyedProcessFunction<Integer, StockMinuteData, StockQuotation> {
    private MapState<String, StockData> stockDataMapState;
    private OutputTag<StockData> stockEodTag;

    @Override
    public void open(Configuration parameters) throws Exception {

        stockEodTag = new OutputTag<StockData>("stock-eod-output") {
        };

        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Time.hours(6)).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();

        MapStateDescriptor<String, StockData> stringStockDataMapStateDescriptor = new MapStateDescriptor<>(
                "stock_last_minute_data", Types.STRING, Types.POJO(StockData.class));
        stringStockDataMapStateDescriptor.enableTimeToLive(ttlConfig);
        stockDataMapState = getRuntimeContext().getMapState(stringStockDataMapStateDescriptor);

    }



    @Override
    public void processElement(StockMinuteData value,
                               KeyedProcessFunction<Integer, StockMinuteData, StockQuotation>.Context ctx,
                               Collector<StockQuotation> out) throws Exception {
        Map<String, StockData> currentDataMap = value.getMinuteDataMap();
        for (String stock : currentDataMap.keySet()) {
            StockData stockData = currentDataMap.get(stock);
            StockData last = stockDataMapState.get(stock);
            out.collect(new StockQuotation(stockData, last));
        }
        // 处理每日个股收盘价
        if (value.getTradeTime() == Constants.MINUTE_15_00) {
            for (StockData stockData : currentDataMap.values()) {
                ctx.output(stockEodTag, stockData);
            }
            for (String key : stockDataMapState.keys()) {
                if (!currentDataMap.containsKey(key)) {
                    ctx.output(stockEodTag, stockDataMapState.get(key));
                }
            }
            stockDataMapState.clear();
        }
    }
}
