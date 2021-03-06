package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.function.TradeDayKeyedBroadcastProcessFunction;
import ink.haifeng.quotation.model.dto.BasicInfoData;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 如果当日不是交易日，这过滤交易数据，如果是则往下游传输
 *
 * @author haifeng
 */
public class TradeDayWithOutputProcessFunction implements WithOutputHandler<SingleOutputStreamOperator<StockData>,
        SingleOutputStreamOperator<StockData>> {
    private BroadcastStream<BasicInfoData> broadcastStream;

    public TradeDayWithOutputProcessFunction(BroadcastStream<BasicInfoData> broadcastStream) {
        this.broadcastStream = broadcastStream;
    }

    @Override
    public SingleOutputStreamOperator<StockData> handler(SingleOutputStreamOperator<StockData> stream,
                                                         Properties properties) {
        return stream.keyBy(StockData::getTradeDay)
                .connect(broadcastStream)
                .process(new TradeDayKeyedBroadcastProcessFunction());
    }
}
