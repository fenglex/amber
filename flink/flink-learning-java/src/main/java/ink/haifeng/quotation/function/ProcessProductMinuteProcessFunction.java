package ink.haifeng.quotation.function;

import ink.haifeng.dto.ProductData;
import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.common.DateUtils;
import ink.haifeng.quotation.model.dto.ProductConstituentsInfo;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockMinutePreData;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessProductMinuteProcessFunction extends KeyedBroadcastProcessFunction<String, StockMinutePreData, List<ProductConstituentsInfo>, ProductData> {


    private ListState<ProductData> productDataCache;

    private MapState<String, List<ProductConstituentsInfo>> stockConstituentsMapState;

    private ValueState<Integer> currentConstituentsDayState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<ProductData> cacheProductDataListStateDescriptor =
                new ListStateDescriptor<ProductData>("product-data-cache", Types.POJO(ProductData.class));
        productDataCache = getRuntimeContext().getListState(cacheProductDataListStateDescriptor);
        stockConstituentsMapState = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("stock-constituents", Types.STRING, Types.LIST(Types.POJO(ProductConstituentsInfo.class))));
        currentConstituentsDayState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("current-day", Types.INT));
    }


    @Override
    public void processElement(StockMinutePreData stockData, KeyedBroadcastProcessFunction<String, StockMinutePreData, List<ProductConstituentsInfo>, ProductData>.ReadOnlyContext readOnlyContext, Collector<ProductData> collector) throws Exception {
        StockData current = stockData.getCurrent();
        Integer currentConstituentsDay = currentConstituentsDayState.value();
        ProductData productData = new ProductData(current, stockData.getLastMinute(), null);
        productDataCache.add(productData);
        if (currentConstituentsDay != null && current.getTradeDay() != currentConstituentsDay) {
            Iterable<ProductData> dataIterable = productDataCache.get();
            dataIterable.forEach(e -> {
                String stockCode = e.getCurrent().getStockCode();
                List<ProductConstituentsInfo> infos;
                try {
                    infos = stockConstituentsMapState.get(stockCode);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                if (infos != null && !infos.isEmpty()) {
                    for (ProductConstituentsInfo info : infos) {
                        ProductData data = new ProductData(e.getCurrent(), e.getLastMinute(), info);
                        collector.collect(data);
                    }
                }
            });
            productDataCache.clear();
        }
    }

    @Override
    public void processBroadcastElement(List<ProductConstituentsInfo> infos, KeyedBroadcastProcessFunction<String, StockMinutePreData, List<ProductConstituentsInfo>, ProductData>.Context context, Collector<ProductData> collector) throws Exception {
        ExecutionConfig.GlobalJobParameters parameters =
                this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ParameterTool params = (ParameterTool) parameters;
        int runDay = params.getInt(Constants.RUN_DAY, 0);
        int currentDay = runDay != 0 ? runDay : DateUtils.currentDay();

        for (ProductConstituentsInfo constituentsInfo : infos) {
            List<ProductConstituentsInfo> stateInfos = stockConstituentsMapState.get(constituentsInfo.getStockCode());
            if (stateInfos == null) {
                stateInfos = new ArrayList<>();
            }
            stateInfos.add(constituentsInfo);
            infos = infos.stream().filter(e -> e.getTradeDay() == currentDay).collect(Collectors.toList());
            stockConstituentsMapState.put(constituentsInfo.getStockCode(), infos);
        }
        currentConstituentsDayState.update(currentDay);
    }
}
