package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.common.DateUtils;
import ink.haifeng.quotation.model.dto.RedisValue;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.sink.RedisValueSink;
import ink.haifeng.quotation.sink.SinkFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Properties;

/**
 * 处理个股实时数据（实际为30秒）
 *
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/19 18:22:30
 */
public class StockRealTimeNoOutputHandler implements NoOutputHandler<SingleOutputStreamOperator<StockData>> {
    @Override
    public void handler(SingleOutputStreamOperator<StockData> stream, Properties properties) {
        RedisValueSink redisValueSink = SinkFactory.getSink(RedisValueSink.class, properties);
        stream.filter(e -> e.getState() != -2)
                .keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) > Long.parseLong(value2.getRealtime()) ? value1 : value2)
                .filter(e -> DateUtils.isTradeTime(e.minute()))
                .map((MapFunction<StockData, RedisValue>) value -> new RedisValue(Constants.STOCK_REDIS_KEY,
                        value.getCodePrefix(), value.redisString()))
                .addSink(redisValueSink.sink());
    }
}
