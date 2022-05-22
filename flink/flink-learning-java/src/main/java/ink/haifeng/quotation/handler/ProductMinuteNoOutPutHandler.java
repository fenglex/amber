package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class ProductMinuteNoOutPutHandler implements NoOutputHandler<SingleOutputStreamOperator<StockDataWithPre>> {

    private BroadcastStream<BasicInfoData> broadcastStream;

    public ProductMinuteNoOutPutHandler(BroadcastStream<BasicInfoData> broadcastStream) {
        this.broadcastStream = broadcastStream;
    }

    @Override
    public void handler(SingleOutputStreamOperator<StockDataWithPre> stream, Properties properties) {

        SingleOutputStreamOperator<StockRelateProductData> process = stream.keyBy(e -> e.getCurrent().getStockCode())
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, StockDataWithPre, BasicInfoData,
                        StockRelateProductData>() {

                    private ValueState<BasicInfoData> basicInfoDataValueState;
                    private ListState<StockDataWithPre> cacheListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<BasicInfoData> tradeDayStateDescriptor = new ValueStateDescriptor<>(
                                "product_trade_day_state", Types.POJO(BasicInfoData.class));
                        basicInfoDataValueState = getRuntimeContext().getState(tradeDayStateDescriptor);

                        ListStateDescriptor<StockDataWithPre> listStateDescriptor = new ListStateDescriptor<>(
                                "minute_product_state", Types.POJO(StockDataWithPre.class));
                        cacheListState = getRuntimeContext().getListState(listStateDescriptor);
                    }

                    @Override
                    public void processElement(StockDataWithPre value, KeyedBroadcastProcessFunction<String,
                            StockDataWithPre
                            , BasicInfoData, StockRelateProductData>.ReadOnlyContext ctx,
                                               Collector<StockRelateProductData> out) throws Exception {
                        System.out.println(value);
                        BasicInfoData basicInfo = basicInfoDataValueState.value();
                        cacheListState.add(value);
                        if (basicInfo != null && basicInfo.getTradeDay() == value.getCurrent().getTradeDay()) {
                            for (StockDataWithPre dataWithPre : cacheListState.get()) {
                                StockData current = dataWithPre.getCurrent();
                                if (current.getTradeDay() == basicInfo.getTradeDay()) {
                                    for (ProductInfo info : basicInfo.getInfos()) {
                                        for (ProductConstituents constituent : info.getConstituents()) {
                                            if (constituent.getStockCode().equals(value.getCurrent().getStockCode())) {
                                                out.collect(new StockRelateProductData(info.getProductCode(), value,
                                                        info));
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(BasicInfoData value, KeyedBroadcastProcessFunction<String,
                            StockDataWithPre, BasicInfoData, StockRelateProductData>.Context ctx,
                                                        Collector<StockRelateProductData> out) throws Exception {
                        basicInfoDataValueState.update(value);
                    }
                });

        SingleOutputStreamOperator<ProductQuotation> productMinute =
                process.keyBy(StockRelateProductData::getProductCode).window(TumblingEventTimeWindows.of(Time.minutes(1))).aggregate(new AggregateFunction<StockRelateProductData, List<StockRelateProductData>, ProductQuotation>() {
                    @Override
                    public List<StockRelateProductData> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<StockRelateProductData> add(StockRelateProductData stockRelateProductData,
                                                            List<StockRelateProductData> list) {
                        list.add(stockRelateProductData);
                        return list;
                    }

                    @Override
                    public ProductQuotation getResult(List<StockRelateProductData> stockRelateProductData) {
                        ProductQuotation quotation = new ProductQuotation();
                        BigDecimal sum = new BigDecimal("0");
                        long amount = 0L, totalAmount = 0L;
                        Set<String> stockSet = new HashSet<>();
                        ProductInfo productInfo = null;
                        int tradeTime = 0;
                        for (StockRelateProductData relateProductData : stockRelateProductData) {
                            StockDataWithPre data = relateProductData.getData();
                            StockData current = data.getCurrent();
                            if (tradeTime != 0 && tradeTime != current.minute()) {
                                System.out.println("产品分钟数据异常");
                            }
                            tradeTime = current.minute();
                            StockData lastData = data.getLastMinute();
                            productInfo = relateProductData.getProductInfo();
                            stockSet.add(current.getStockCode());
                            totalAmount += current.getAmount();
                            amount += current.getAmount() - lastData.getAmount();
                            for (ProductConstituents constituent : productInfo.getConstituents()) {
                                if (constituent.getStockCode().equals(current.getStockCode())) {
                                    sum = sum.add(current.getPrice()).multiply(new BigDecimal(constituent.getAdjShare() + ""));
                                    break;
                                }
                            }
                        }
                        assert productInfo != null;
                        List<ProductConstituents> constituents = productInfo.getConstituents();
                        Map<String, StockPreClosePrice> stockPreCloseMap = productInfo.getStockPreClose();
                        if (stockSet.size() < constituents.size()) {
                            for (ProductConstituents constituent : constituents) {
                                if (!stockSet.contains(constituent.getStockCode())) {
                                    StockPreClosePrice stockPreClosePrice =
                                            stockPreCloseMap.get(constituent.getStockCode());
                                    String log = "use preClose:productCode:%s stockCode:%s tradeDay:%s " + "price:%s";
                                    if (stockPreClosePrice != null) {
                                        sum = sum.add(stockPreClosePrice.getPreClose()).multiply(new BigDecimal(constituent.getAdjShare() + ""));
                                        log = String.format(log, productInfo.getProductCode(),
                                                constituent.getStockCode(),
                                                stockPreClosePrice.getTradeDay(), stockPreClosePrice.getPreClose());
                                    } else {
                                        log = String.format(log, productInfo.getProductCode(),
                                                constituent.getStockCode(),
                                                null, null);
                                    }
                                    System.out.println(log);
                                }
                            }
                        }
                        ProductBasicInfo basicInfo = productInfo.getBasicInfo();
                        quotation.setProductCode(productInfo.getProductCode());
                        quotation.setProductName(basicInfo.getProductName());
                        quotation.setTradeDay(basicInfo.getTradeDay());
                        quotation.setAmount(amount);
                        quotation.setTotalAmount(totalAmount);
                        BigDecimal price =
                                sum.divide(basicInfo.getDivisor(), 10, RoundingMode.HALF_UP).multiply(new BigDecimal("1000"));
                        quotation.setPrice(price);
                        quotation.setPreClose(basicInfo.getClosePrice());
                        System.out.println(quotation);
                        return quotation;
                    }

                    @Override
                    public List<StockRelateProductData> merge(List<StockRelateProductData> stockRelateProductData,
                                                              List<StockRelateProductData> acc1) {
                        stockRelateProductData.addAll(acc1);
                        return stockRelateProductData;
                    }
                });

        productMinute.print("product_minute");

    }
}
