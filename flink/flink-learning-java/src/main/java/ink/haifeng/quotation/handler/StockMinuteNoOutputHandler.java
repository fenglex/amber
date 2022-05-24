package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.function.StockMinuteKeyedBroadcastProcessFunction;
import ink.haifeng.quotation.model.dto.*;
import ink.haifeng.quotation.model.entity.ProductEod;
import ink.haifeng.quotation.model.entity.StockQuotation;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
        SingleOutputStreamOperator<StockQuotation> minuteStream = stream.keyBy(StockMinuteData::getTradeDay)
                .connect(broadcastStream).process(new StockMinuteKeyedBroadcastProcessFunction());
        OutputTag<StockData> stockEodTag = new OutputTag<StockData>("stock-eod-output") {
        };
        OutputTag<ProductQuotation> productQuotationOutputTag = new OutputTag<ProductQuotation>("product-minute" +
                "-output") {
        };
        minuteStream.getSideOutput(stockEodTag).print("stock_eod");

        minuteStream.getSideOutput(productQuotationOutputTag).print("productQuotationOutputTag");

        OutputTag<ProductEod> productEodOutputTag = new OutputTag<ProductEod>("product-eod-output") {
        };
        minuteStream.getSideOutput(productEodOutputTag).print("product-eod-tag");
    }
}
