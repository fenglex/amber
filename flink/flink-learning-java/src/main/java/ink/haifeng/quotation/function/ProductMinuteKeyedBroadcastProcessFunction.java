package ink.haifeng.quotation.function;

import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.model.entity.StockQuotation;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

public class ProductMinuteKeyedBroadcastProcessFunction
        extends KeyedBroadcastProcessFunction<Integer, MinuteStockQuotation, BasicInfoData, ProductQuotation> {
    private MapStateDescriptor<Integer, BasicInfoData> basicInfoBroadcastStateDescriptor;

    private MapState<String, StockQuotation> stockQuotationMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(6))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        basicInfoBroadcastStateDescriptor = new MapStateDescriptor<>("basic_info_broadcast_state", Types.INT,
                Types.POJO(BasicInfoData.class));
        basicInfoBroadcastStateDescriptor.enableTimeToLive(ttlConfig);
        MapStateDescriptor<String, StockQuotation> mapStateDescriptor = new MapStateDescriptor<>(
                "stock-last-quotation", Types.STRING,
                Types.POJO(StockQuotation.class));
        mapStateDescriptor.enableTimeToLive(ttlConfig);
        stockQuotationMapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(MinuteStockQuotation value, KeyedBroadcastProcessFunction<Integer,
            MinuteStockQuotation, BasicInfoData, ProductQuotation>.ReadOnlyContext ctx,
                               Collector<ProductQuotation> out) throws Exception {
        BasicInfoData basicInfo =
                ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(ctx.getCurrentKey());
        for (ProductInfo info : basicInfo.getInfos()) {
            ProductQuotation productQuotation = handlerProductQuotation(info, value);
            out.collect(productQuotation);
        }
    }


    private ProductQuotation handlerProductQuotation(ProductInfo product, MinuteStockQuotation value) throws Exception {
        Map<String, StockQuotation> quotationMap = value.getQuotationMap();
        ProductBasicInfo basicInfo = product.getBasicInfo();
        Map<String, ProductConstituents> constituents = product.getConstituents();
        Map<String, StockPreClosePrice> stockPreClose = product.getStockPreClose();
        long amount = 0, totalAmount = 0;
        BigDecimal sum = new BigDecimal("0");
        for (String stock : constituents.keySet()) {
            ProductConstituents cons = constituents.get(stock);
            StockQuotation quotation = quotationMap.get(stock);
            StockPreClosePrice stockPreClosePrice = stockPreClose.getOrDefault(stock, new StockPreClosePrice());
            StockQuotation lastData = stockQuotationMapState.get(stock);
            if (quotation != null) {
                totalAmount += quotation.getTotalAmount();
                amount += quotation.getAmount();
                sum = sum.add(quotation.getPrice().multiply(new BigDecimal(cons.getAdjShare() + "")));
            } else if (lastData != null) {
                totalAmount += lastData.getTotalAmount();
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

    @Override
    public void processBroadcastElement(BasicInfoData value, KeyedBroadcastProcessFunction<Integer,
            MinuteStockQuotation, BasicInfoData, ProductQuotation>.Context ctx,
                                        Collector<ProductQuotation> out) throws Exception {
        ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).put(value.getTradeDay(), value);
    }
}
