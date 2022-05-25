package ink.haifeng.quotation.function;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.model.entity.ProductEod;
import ink.haifeng.quotation.model.entity.StockQuotation;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

public class ProductMinuteKeyedBroadcastProcessFunction
        extends KeyedBroadcastProcessFunction<Integer, MinuteStockQuotation, BasicInfoData, ProductQuotation> {
    private MapStateDescriptor<Integer, BasicInfoData> basicInfoBroadcastStateDescriptor;

    private MapState<String, StockQuotation> stockQuotationMapState;

    private MapState<String, ProductPrice> productPriceMapState;

    private OutputTag<ProductEod> productEodOutputTag;

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
        MapStateDescriptor<String, ProductPrice> productPriceState = new MapStateDescriptor<>("product_price_state"
                , Types.STRING, Types.POJO(ProductPrice.class));
        productPriceState.enableTimeToLive(ttlConfig);
        productPriceMapState = getRuntimeContext().getMapState(productPriceState);

        productEodOutputTag = new OutputTag<ProductEod>("product-eod-output") {
        };

    }

    @Override
    public void processElement(MinuteStockQuotation value, KeyedBroadcastProcessFunction<Integer,
            MinuteStockQuotation, BasicInfoData, ProductQuotation>.ReadOnlyContext ctx,
                               Collector<ProductQuotation> out) throws Exception {
        ReadOnlyBroadcastState<Integer, BasicInfoData> broadcastState =
                ctx.getBroadcastState(basicInfoBroadcastStateDescriptor);
        BasicInfoData basicInfo = broadcastState.get(ctx.getCurrentKey());
        for (ProductInfo info : basicInfo.getInfos()) {
            ProductQuotation productQuotation = handlerProductQuotation(info, value);
            out.collect(productQuotation);
            if (value.getTradeTime() == Constants.MINUTE_15_00) {
                ProductPrice productPrice = productPriceMapState.get(info.getProductCode());
                ctx.output(productEodOutputTag, new ProductEod(info.getBasicInfo(),
                        productQuotation.getTotalAmount(), productQuotation.getPrice(), productPrice));
            }
        }
        if (value.getTradeTime() == Constants.MINUTE_15_00) {
            productPriceMapState.clear();
            stockQuotationMapState.clear();
            broadcastState.clear();
        }
    }


    private ProductQuotation handlerProductQuotation(ProductInfo product, MinuteStockQuotation value) throws Exception {
        Map<String, StockQuotation> quotationMap = value.getQuotationMap();
        ProductBasicInfo basicInfo = product.getBasicInfo();
        Map<String, ProductConstituents> constituents = product.getConstituents();
        Map<String, StockPreClosePrice> stockPreClose = product.getStockPreClose();
        long amount = 0, totalAmount = 0;
        BigDecimal sum = new BigDecimal("0");
        int count = 0;
        for (String stock : constituents.keySet()) {
            ProductConstituents cons = constituents.get(stock);
            StockQuotation quotation = quotationMap.get(stock);
            StockPreClosePrice stockPreClosePrice = stockPreClose.getOrDefault(stock, new StockPreClosePrice());
            StockQuotation lastData = stockQuotationMapState.get(stock);
            if (quotation != null) {
                totalAmount += quotation.getTotalAmount();
                amount += quotation.getAmount();
                sum = sum.add(quotation.getPrice().multiply(new BigDecimal(cons.getAdjShare() + "")));
                count += 1;
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
        if (count > 0) {
            String productCode = product.getProductCode();
            ProductPrice productPrice = productPriceMapState.get(productCode);
            if (productPrice == null) {
                productPrice = new ProductPrice();
            }
            if (price.compareTo(BigDecimal.ZERO) > 0) {
                productPrice.update(price);
            }
        }
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
