package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockDataWithPre;
import ink.haifeng.quotation.model.dto.StockMinuteData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 将数据转换为每分钟的数据
 *
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 11:01:02
 */
public class ToMinuteDataWithOutputHandler implements WithOutputHandler<SingleOutputStreamOperator<StockData>,
        SingleOutputStreamOperator<StockMinuteData>> {
    @Override
    public SingleOutputStreamOperator<StockMinuteData> handler(SingleOutputStreamOperator<StockData> stream,
                                                               Properties properties) {

        SingleOutputStreamOperator<StockMinuteData> aggregateStream = stream.keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).allowedLateness(Time.seconds(10))
                .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value2.getRealtime()) >= Long.parseLong(value1.getRealtime()) ? value2 : value1)
                .keyBy(StockData::getTradeDay)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).aggregate(new AggregateFunction<StockData,
                        List<StockData>, StockMinuteData>() {
                    @Override
                    public List<StockData> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<StockData> add(StockData value, List<StockData> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public StockMinuteData getResult(List<StockData> accumulator) {
                        StockData data = accumulator.get(0);
                        Map<String, StockData> stockDataMap =
                                accumulator.stream().collect(Collectors.toMap(StockData::getStockCode, e -> e));
                        return new StockMinuteData(data.getTradeDay(), data.minute(), stockDataMap);
                    }

                    @Override
                    public List<StockData> merge(List<StockData> a, List<StockData> b) {
                        a.addAll(b);
                        return a;
                    }
                });

        return aggregateStream;
    }
}
