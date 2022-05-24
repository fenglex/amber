package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * @author haifeng
 */
public class ProductMinuteNoOutPutHandler implements NoOutputHandler<SingleOutputStreamOperator<StockDataWithPre>> {

    private BroadcastStream<BasicInfoData> broadcastStream;

    public ProductMinuteNoOutPutHandler(BroadcastStream<BasicInfoData> broadcastStream) {
        this.broadcastStream = broadcastStream;
    }


    /**
     * 将数据与其关联产品相关联
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<StockRelateProductData> processRelateProduct(SingleOutputStreamOperator<StockDataWithPre> stream) {
      /*  return stream.keyBy(e -> e.getCurrent().getTradeDay())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .aggregate(new AggregateFunction<StockDataWithPre, List<StockDataWithPre>, ProductMinuteData>() {

                    @Override
                    public List<StockDataWithPre> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<StockDataWithPre> add(StockDataWithPre value, List<StockDataWithPre> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public ProductMinuteData getResult(List<StockDataWithPre> accumulator) {
                        int tradeDay = accumulator.get(0).getCurrent().getTradeDay();
                        return new ProductMinuteData(tradeDay, accumulator);
                    }

                    @Override
                    public List<StockDataWithPre> merge(List<StockDataWithPre> a, List<StockDataWithPre> b) {
                        a.addAll(b);
                        return a;
                    }
                }).keyBy(ProductMinuteData::getTradeDay)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, ProductMinuteData, BasicInfoData,
                        StockRelateProductData>() {

                    @Override
                    public void processElement(ProductMinuteData value, KeyedBroadcastProcessFunction<String,
                            ProductMinuteData, BasicInfoData, StockRelateProductData>.ReadOnlyContext ctx,
                                               Collector<StockRelateProductData> out) throws Exception {

                    }

                    @Override
                    public void processBroadcastElement(BasicInfoData value, KeyedBroadcastProcessFunction<String,
                            ProductMinuteData, BasicInfoData, StockRelateProductData>.Context ctx, Collector<StockRelateProductData> out) throws Exception {

                    }

                    private ListState<StockDataWithPre> cacheListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<StockDataWithPre> listStateDescriptor = new ListStateDescriptor<>(
                                "minute_product_state", Types.POJO(StockDataWithPre.class));
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(6))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .neverReturnExpired()
                                .build();
                        listStateDescriptor.enableTimeToLive(ttlConfig);
                        cacheListState = getRuntimeContext().getListState(listStateDescriptor);
                    }



                    @Override
                    public void processElement(StockDataWithPre value, KeyedBroadcastProcessFunction<String,
                            StockDataWithPre
                            , BasicInfoData, StockRelateProductData>.ReadOnlyContext ctx,
                                               Collector<StockRelateProductData> out) throws Exception {

                        MapStateDescriptor<Void, BasicInfoData> basicInfoBroadcastStateDescriptor =
                                new MapStateDescriptor<>(
                                        "basic_info_broadcast_state", Types.VOID, Types.POJO(BasicInfoData.class));

                        BasicInfoData basicInfo = ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).get(null);
                        cacheListState.add(value);
                        if (basicInfo == null) {
                            System.out.println("info-data-empty");
                            ctx.timerService().registerProcessingTimeTimer(ctx.currentProcessingTime() + 30000L);
                        } else {
                            for (StockDataWithPre dataWithPre : cacheListState.get()) {
                                StockData current = dataWithPre.getCurrent();
                                if (current.getTradeDay() == basicInfo.getTradeDay()) {
                                    for (ProductInfo info : basicInfo.getInfos()) {
                                        Map<String, ProductConstituents> constituentsMap = info.getConstituents();
                                        ProductConstituents productConstituents =
                                                constituentsMap.get(current.getStockCode());
                                        if (productConstituents != null) {
                                            out.collect(new StockRelateProductData(info.getProductCode(), dataWithPre
                                                    , info));
                                        }
                                    }
                                }
                            }
                            cacheListState.clear();
                        }
                    }

                    @Override
                    public void processBroadcastElement(BasicInfoData value, KeyedBroadcastProcessFunction<String,
                            StockDataWithPre, BasicInfoData, StockRelateProductData>.Context ctx,
                                                        Collector<StockRelateProductData> out) throws Exception {
                        MapStateDescriptor<Void, BasicInfoData> basicInfoBroadcastStateDescriptor =
                                new MapStateDescriptor<>(
                                        "basic_info_broadcast_state", Types.VOID, Types.POJO(BasicInfoData.class));
                        ctx.getBroadcastState(basicInfoBroadcastStateDescriptor).put(null, value);
                    }
                });*/
        return null;
    }

    @Override
    public void handler(SingleOutputStreamOperator<StockDataWithPre> stream, Properties properties) {
       /* processRelateProduct(stream).keyBy(StockRelateProductData::getTradeDay)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new AggregateFunction<StockRelateProductData, List<StockRelateProductData>,
                        List<StockRelateProductData>>() {
                    @Override
                    public List<StockRelateProductData> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<StockRelateProductData> add(StockRelateProductData value,
                                                            List<StockRelateProductData> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public List<StockRelateProductData> getResult(List<StockRelateProductData> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public List<StockRelateProductData> merge(List<StockRelateProductData> a,
                                                              List<StockRelateProductData> b) {
                        a.addAll(b);
                        return a;
                    }
                }).map(new RichMapFunction<List<StockRelateProductData>, ProductQuotation>() {
                    private MapState<String, StockData> stockDataMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, StockData> stockDataMapStateDescriptor =
                                new MapStateDescriptor<>("product_stock_cache_state", Types.STRING,
                                        Types.POJO(StockData.class));
                        stockDataMapState = getRuntimeContext().getMapState(stockDataMapStateDescriptor);
                    }

                    @Override
                    public ProductQuotation map(List<StockRelateProductData> value) throws Exception {
                        BigDecimal sum = new BigDecimal("0");
                        long amount = 0, totalAmount = 0;
                        Set<String> stockSet = new HashSet<>();
                        Map<String, ProductConstituents> constituentsMap = null;
                        Map<String, StockPreClosePrice> preClosePriceMap = null;
                        ProductBasicInfo basicInfo = null;
                        int tradeDay = 0, tradeTime = 0;
                        for (StockRelateProductData data : value) {
                            ProductInfo productInfo = data.getProductInfo();
                            basicInfo = productInfo.getBasicInfo();
                            constituentsMap = productInfo.getConstituents();
                            StockDataWithPre stockDataWithPre = data.getData();
                            StockData current = stockDataWithPre.getCurrent();
                            tradeDay = current.getTradeDay();
                            tradeTime = current.minute();
                            StockData last = stockDataWithPre.getLastMinute();
                            preClosePriceMap = productInfo.getStockPreClose();
                            totalAmount += current.getAmount();
                            if (last != null) {
                                amount += current.getAmount() - last.getAmount();
                            } else {
                                amount += current.getAmount();
                            }
                            ProductConstituents constituents = constituentsMap.get(current.getStockCode());
                            sum = sum.add(current.getPrice()).multiply(new BigDecimal(constituents.getAdjShare() + ""));
                            stockSet.add(current.getStockCode());
                        }
                        assert constituentsMap != null;
                        if (stockSet.size() < constituentsMap.size()) {
                            for (String stock : constituentsMap.keySet()) {
                                if (!stockSet.contains(stock)) {
                                    ProductConstituents constituents = constituentsMap.get(stock);
                                    StockData stockData = stockDataMapState.get(stock);
                                    if (stockData != null) {
                                        sum = sum.add(stockData.getPrice())
                                                .multiply(new BigDecimal(constituents.getAdjShare() + ""));
                                    } else {
                                        StockPreClosePrice preClosePrice = preClosePriceMap.getOrDefault(stock,
                                                new StockPreClosePrice());
                                        sum = sum.add(preClosePrice.getPreClose())
                                                .multiply(new BigDecimal(constituents.getAdjShare() + ""));
                                    }
                                }
                            }
                        }
                        for (StockRelateProductData data : value) {
                            StockData current = data.getData().getCurrent();
                            stockDataMapState.put(current.getStockCode(), current);
                        }
                        List<String> removeKeys = new ArrayList<>();
                        for (StockData stockData : stockDataMapState.values()) {
                            if (stockData.getTradeDay() != tradeDay) {
                                removeKeys.add(stockData.getStockCode());
                            }
                        }
                        for (String removeKey : removeKeys) {
                            stockDataMapState.remove(removeKey);
                        }
                        ProductQuotation quotation = new ProductQuotation();
                        BigDecimal price = sum.divide(basicInfo.getDivisor(), 10, RoundingMode.HALF_UP)
                                .multiply(new BigDecimal("1000"));
                        quotation.setProductCode(basicInfo.getProductCode());
                        quotation.setProductName(basicInfo.getProductName());
                        quotation.setTradeDay(tradeDay);
                        quotation.setTradeTime(tradeTime);
                        quotation.setPrice(price);
                        quotation.setPreClose(basicInfo.getClosePrice());
                        quotation.setAmount(amount);
                        quotation.setTotalAmount(totalAmount);
                        return quotation;
                    }
                }).print("product-test");*/

    }
}
