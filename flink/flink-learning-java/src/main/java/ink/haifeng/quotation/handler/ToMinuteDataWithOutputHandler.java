package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockDataWithPre;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Properties;

/**
 * 将数据转换为每分钟的数据
 *
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 11:01:02
 */
public class ToMinuteDataWithOutputHandler implements WithOutputHandler<SingleOutputStreamOperator<StockData>,
        SingleOutputStreamOperator<StockDataWithPre>> {
    @Override
    public SingleOutputStreamOperator<StockDataWithPre> handler(SingleOutputStreamOperator<StockData> stream,
                                                                Properties properties) {
        SingleOutputStreamOperator<StockDataWithPre> stockMinuteData =
                stream.keyBy(StockData::getStockCode)
                        .window(TumblingEventTimeWindows.of(Time.minutes(1))).allowedLateness(Time.seconds(10))
                        .reduce((ReduceFunction<StockData>) (value1, value2) -> Long.parseLong(value1.getRealtime()) <= Long.parseLong(value2.getRealtime()) ? value2 : value1)
                        .keyBy(StockData::getStockCode)
                        .map(new RichMapFunction<StockData, StockDataWithPre>() {
                            private ValueState<StockData> stockLastMinuteState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<StockData> valueStateDescriptor = new ValueStateDescriptor<>(
                                        "last_stock_minute", Types.POJO(StockData.class));
                                stockLastMinuteState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public StockDataWithPre map(StockData value) throws Exception {
                                StockData last = stockLastMinuteState.value();
                                StockDataWithPre data;
                                if (last != null && last.getTradeDay() == value.getTradeDay()) {
                                    // TODO drop
                              /*      System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s",
                                            value.getStockCode(), value.minute(), value.getRealtime(),
                                            last.getStockCode(), last.minute(), last.getRealtime()));*/
                                    data = new StockDataWithPre(value, last);
                                } else {
                                    data = new StockDataWithPre(value, null);
                                }
                                stockLastMinuteState.update(value);
                                return data;
                            }
                        }).returns(Types.POJO(StockDataWithPre.class));

        return stockMinuteData;
    }
}
