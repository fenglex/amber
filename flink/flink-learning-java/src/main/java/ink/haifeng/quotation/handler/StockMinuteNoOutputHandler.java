package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.function.ProductMinuteKeyedBroadcastProcessFunction;
import ink.haifeng.quotation.function.StockMinuteKeyedProcessFunction;
import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.model.entity.ProductEod;
import ink.haifeng.quotation.model.entity.StockDaily;
import ink.haifeng.quotation.model.entity.StockQuotation;
import ink.haifeng.quotation.sink.ProductQuotationSink;
import ink.haifeng.quotation.sink.StockDailySink;
import ink.haifeng.quotation.sink.StockQuotationSink;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 处理个股分钟数据（写入mysql）
 *
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/19 18:29:14
 */
public class StockMinuteNoOutputHandler implements NoOutputHandler<SingleOutputStreamOperator<StockMinuteData>> {

    private final BroadcastStream<BasicInfoData> broadcastStream;


    public StockMinuteNoOutputHandler(BroadcastStream<BasicInfoData> broadcastStream) {
        this.broadcastStream = broadcastStream;
    }


    @Override
    public void handler(SingleOutputStreamOperator<StockMinuteData> stream, Properties properties) {
        SingleOutputStreamOperator<StockQuotation> minuteStream =
                stream.keyBy(StockMinuteData::getTradeDay).process(new StockMinuteKeyedProcessFunction());
        OutputTag<StockData> stockEodTag = new OutputTag<StockData>("stock-eod-output") {
        };

        OutputTag<ProductQuotation> productQuotationOutputTag = new OutputTag<ProductQuotation>("product-minute" +
                "-output") {
        };

        OutputTag<ProductEod> productEodOutputTag = new OutputTag<ProductEod>("product-eod-output") {
        };


        SingleOutputStreamOperator<ProductQuotation> productMinute =
                minuteStream.keyBy(StockQuotation::getTradeDay).window(TumblingEventTimeWindows.of(Time.minutes(1))).aggregate(new AggregateFunction<StockQuotation, List<StockQuotation>, MinuteStockQuotation>() {
            @Override
            public List<StockQuotation> createAccumulator() {
                return new ArrayList<>();
            }

            @Override
            public List<StockQuotation> add(StockQuotation value, List<StockQuotation> accumulator) {
                accumulator.add(value);
                return accumulator;
            }

            @Override
            public MinuteStockQuotation getResult(List<StockQuotation> accumulator) {
                StockQuotation quotation = accumulator.get(0);
                Map<String, StockQuotation> quotationMap =
                        accumulator.stream().collect(Collectors.toMap(StockQuotation::getStockCode, e -> e));
                return new MinuteStockQuotation(quotation.getTradeDay(), quotation.getTradeTime(), quotationMap);
            }

            @Override
            public List<StockQuotation> merge(List<StockQuotation> a, List<StockQuotation> b) {
                return null;
            }
        }).keyBy(MinuteStockQuotation::getTradeDay).connect(broadcastStream).process(new ProductMinuteKeyedBroadcastProcessFunction());

        //stock minute
        StockQuotationSink stockQuotationSink = new StockQuotationSink(properties);
        //minuteStream.addSink(stockQuotationSink.sink());
        minuteStream.print("stock_minute");

        // stock eod
        StockDailySink stockDailySink = new StockDailySink(properties);
        SinkFunction<StockDaily> sink = stockDailySink.sink();
        //minuteStream.getSideOutput(stockEodTag).map(StockDaily::new).addSink(stockDailySink.sink());
        minuteStream.getSideOutput(stockEodTag).print("stock_eod");

        // product minute
        productMinute.print("product-minute-quotation");
        ProductQuotationSink productQuotationSink = new ProductQuotationSink(properties);
        minuteStream.getSideOutput(productQuotationOutputTag).print("productQuotationOutputTag");

        // product eod
        //ProductEodSink productEodSink = new ProductEodSink(properties);
        //minuteStream.getSideOutput(productEodOutputTag).addSink(productEodSink.sink());
        //minuteStream.getSideOutput(productEodOutputTag).print("product-eod-tag");
    }
}
