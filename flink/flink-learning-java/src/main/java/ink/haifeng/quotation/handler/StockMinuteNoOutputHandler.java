package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.StockDataWithPre;
import ink.haifeng.quotation.model.entity.StockQuotation;
import ink.haifeng.quotation.sink.StockQuotationSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Properties;

/**
 * 处理个股分钟数据（写入mysql）
 *
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/19 18:29:14
 */
public class StockMinuteNoOutputHandler implements NoOutputHandler<SingleOutputStreamOperator<StockDataWithPre>> {
    @Override
    public void handler(SingleOutputStreamOperator<StockDataWithPre> stream, Properties properties) {
        StockQuotationSink stockQuotationSink = new StockQuotationSink(properties);
        stream.map(e -> new StockQuotation(e.getCurrent(), e.getLastMinute())).addSink(stockQuotationSink.sink());
    }
}
