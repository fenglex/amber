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
public class StockMinuteKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<Integer, StockMinuteData,
        BasicInfoData, StockQuotation> {
    private MapStateDescriptor<Integer, BasicInfoData> basicInfoBroadcastStateDescriptor;
    private MapState<String, StockData> stockDataMapState;
    private OutputTag<StockData> stockEodTag;
    private OutputTag<ProductQuotation> productQuotationOutputTag;
    private MapState<String, ProductPrice> productPriceMapState;


    private OutputTag<ProductEod> productEodOutputTag;

    @Override
    public void open(Configuration parameters) throws Exception {

        stockEodTag = new OutputTag<StockData>("stock-eod-output") {
        };
        productQuotationOutputTag = new OutputTag<ProductQuotation>("product-minute-output") {
        };
        productEodOutputTag = new OutputTag<ProductEod>("product-eod-output") {
        };

        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Time.hours(6)).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
        basicInfoBroadcastStateDescriptor = new MapStateDescriptor<>("basic_info_broadcast_state", Types.INT,
                Types.POJO(BasicInfoData.class));
        basicInfoBroadcastStateDescriptor.enableTimeToLive(ttlConfig);

        MapStateDescriptor<String, StockData> stringStockDataMapStateDescriptor = new MapStateDescriptor<>(
                "stock_last_minute_data", Types.STRING, Types.POJO(StockData.class));
        stringStockDataMapStateDescriptor.enableTimeToLive(ttlConfig);
        stockDataMapState = getRuntimeContext().getMapState(stringStockDataMapStateDescriptor);
        MapStateDescriptor<String, ProductPrice> productPriceMapStateDescriptor = new MapStateDescriptor<>("product" +
                "-high-low", Types.STRING, Types.POJO(ProductPrice.class));
        productPriceMapStateDescriptor.enableTimeToLive(ttlConfig);
        productPriceMapState = getRuntimeContext().getMapState(productPriceMapStateDescriptor);
    }

    @Override
    public void processElement(StockMinuteData value, KeyedBroadcastProcessFunction<Integer, StockMinuteData,
            BasicInfoData, StockQuotation>.ReadOnlyContext ctx, Collector<StockQuotation> out) throws Exception {
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
        }

        BasicInfoData basicInfoData = ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(value.getTradeDay());
        List<ProductInfo> products = basicInfoData.getInfos();
        // 处理产品分钟数据
        for (ProductInfo product : products) {
            String productCode = product.getBasicInfo().getProductCode();
            Set<String> productStocks = product.getConstituents().keySet();
            ProductQuotation quotation = handlerProductQuotation(product, value);
            int count = 0;
            for (String stock : productStocks) {
                if (currentDataMap.containsKey(stock)) {
                    count += 1;
                }
            }
            ProductPrice productPrice = productPriceMapState.get(productCode);
            if (productPrice == null) {
                productPrice = new ProductPrice();
            }
            if (count != 0) {
                productPrice.update(quotation.getPrice());
                productPriceMapState.put(productCode, productPrice);
                ctx.output(productQuotationOutputTag, quotation);
            }
            if (value.getTradeTime() == Constants.MINUTE_15_00) {
                ProductEod productEod = new ProductEod(product.getBasicInfo(),quotation.getTotalAmount() ,quotation.getPrice(),
                        productPrice);
                ctx.output(productEodOutputTag, productEod);
            }
        }

        for (String key : currentDataMap.keySet()) {
            StockData stockData = currentDataMap.get(key);
            stockDataMapState.put(key, stockData);
        }
        if (value.getTradeTime() == Constants.MINUTE_15_00) {
            productPriceMapState.clear();
            stockDataMapState.clear();
        }
    }

    @Override
    public void processBroadcastElement(BasicInfoData value, KeyedBroadcastProcessFunction<Integer, StockMinuteData,
            BasicInfoData, StockQuotation>.Context ctx, Collector<StockQuotation> out) throws Exception {
        ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).put(value.getTradeDay(), value);
    }


    private ProductQuotation handlerProductQuotation(ProductInfo product, StockMinuteData value) throws Exception {
        Map<String, StockData> currentDataMap = value.getMinuteDataMap();
        ProductBasicInfo basicInfo = product.getBasicInfo();
        Map<String, ProductConstituents> constituents = product.getConstituents();
        Map<String, StockPreClosePrice> stockPreClose = product.getStockPreClose();
        long amount = 0, totalAmount = 0;
        BigDecimal sum = new BigDecimal("0");
        for (String stock : constituents.keySet()) {
            ProductConstituents cons = constituents.get(stock);
            StockData data = currentDataMap.get(stock);
            StockData lastData = stockDataMapState.get(stock);
            StockPreClosePrice stockPreClosePrice = stockPreClose.getOrDefault(stock, new StockPreClosePrice());
            if (data != null) {
                totalAmount = +data.getAmount();
                if (lastData != null) {
                    amount = data.getAmount() - lastData.getAmount();
                }
                sum = sum.add(data.getPrice().multiply(new BigDecimal(cons.getAdjShare() + "")));
            } else if (lastData != null) {
                totalAmount = +lastData.getAmount();
                sum = sum.add(lastData.getPrice().multiply(new BigDecimal(cons.getAdjShare() + "")));
            } else {
                sum = sum.add(stockPreClosePrice.getPreClose().multiply(new BigDecimal(cons.getAdjShare() + "")));
            }
        }
        ProductQuotation quotation = new ProductQuotation();
        quotation.setProductCode(basicInfo.getProductCode());
        quotation.setProductName(basicInfo.getProductName());
        quotation.setTradeDay(basicInfo.getTradeDay());
        quotation.setTradeTime(value.getTradeTime());
        BigDecimal price = sum.divide(basicInfo.getDivisor(), 10, RoundingMode.HALF_UP).multiply(new BigDecimal("1000"
        ));
        quotation.setPrice(price);
        quotation.setPreClose(basicInfo.getClosePrice());
        quotation.setAmount(amount);
        quotation.setTotalAmount(totalAmount);

        return quotation;
    }
}
