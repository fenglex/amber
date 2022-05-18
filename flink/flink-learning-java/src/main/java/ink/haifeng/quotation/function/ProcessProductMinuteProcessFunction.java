package ink.haifeng.quotation.function;

import ink.haifeng.dto.ProductData;
import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.common.DateUtils;
import ink.haifeng.quotation.model.dto.ProductConstituentsInfo;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockMinuteWithPreData;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class ProcessProductMinuteProcessFunction extends KeyedBroadcastProcessFunction<String, StockMinuteWithPreData, List<ProductConstituentsInfo>, ProductData> {


    private ListState<ProductData> productDataCache;

    private ListState<ProductConstituentsInfo> stockConstituentsListState;

    private ValueState<Integer> currentConstituentsDayState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<ProductData> cacheProductDataListStateDescriptor =
                new ListStateDescriptor<ProductData>("product-data-cache", Types.POJO(ProductData.class));
        productDataCache = getRuntimeContext().getListState(cacheProductDataListStateDescriptor);
        stockConstituentsListState = getRuntimeContext().getListState(new ListStateDescriptor<>("stock-constituents", Types.POJO(ProductConstituentsInfo.class)));


        currentConstituentsDayState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("current-day", Types.INT));
    }


    @Override
    public void processElement(StockMinuteWithPreData stockData,
                               KeyedBroadcastProcessFunction<String, StockMinuteWithPreData, List<ProductConstituentsInfo>, ProductData>.ReadOnlyContext readOnlyContext,
                               Collector<ProductData> collector) throws Exception {
        StockData current = stockData.getCurrent();
        Integer currentConstituentsDay = currentConstituentsDayState.value();
        ProductData productData = new ProductData(stockData.getCurrent(), stockData.getLastMinute(), null);
        productDataCache.add(productData);
        if (currentConstituentsDay != null && current.getTradeDay() == currentConstituentsDay) {
            Iterable<ProductData> dataIterable = productDataCache.get();
            dataIterable.forEach(e -> {
                try {
                    processOneElement(e, collector);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            productDataCache.clear();
        }
    }


    private void processOneElement(ProductData productData, Collector<ProductData> collector) throws Exception {
        Iterable<ProductConstituentsInfo> constituentsInfos = stockConstituentsListState.get();
        constituentsInfos.forEach(e -> {
            if (e.getStockCode().equals(productData.getCurrent().getStockCode())) {
                ProductData data = new ProductData(productData.getCurrent(), productData.getLastMinute(), e);
                System.out.println(data.getCurrent().minute() + "\t" + data.getCurrent().getStockCode());
                collector.collect(data);
            }
        });
    }

    @Override
    public void processBroadcastElement(List<ProductConstituentsInfo> infos, KeyedBroadcastProcessFunction<String, StockMinuteWithPreData, List<ProductConstituentsInfo>, ProductData>.Context context, Collector<ProductData> collector) throws Exception {
        ExecutionConfig.GlobalJobParameters parameters =
                this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ParameterTool params = (ParameterTool) parameters;
        int runDay = params.getInt(Constants.RUN_DAY, 0);
        int currentDay = runDay != 0 ? runDay : DateUtils.currentDay();
        stockConstituentsListState.clear();
        stockConstituentsListState.update(infos);
        currentConstituentsDayState.update(currentDay);
    }
}
