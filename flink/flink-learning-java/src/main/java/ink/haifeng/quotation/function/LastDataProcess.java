package ink.haifeng.quotation.function;

import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 16:18:30
 */
public class LastDataProcess extends KeyedProcessFunction<String, StockData, StockData> {
    private ValueState<StockData> lastDataState;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastDataState = getRuntimeContext().getState(new ValueStateDescriptor<StockData>("last-data",
                StockData.class));
    }

    @Override
    public void processElement(StockData value, KeyedProcessFunction<String, StockData, StockData>.Context ctx,
                               Collector<StockData> out) throws Exception {

        int minute = value.minute();
        if (minute == Constants.MINUTE_9_25 || minute == Constants.MINUTE_11_29 || minute == Constants.MINUTE_15_00) {

            // TODO 修改为晚60秒发送
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5 * 1000L);
        }
        out.collect(value);
        lastDataState.update(value);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, StockData, StockData>.OnTimerContext ctx,
                        Collector<StockData> out) throws Exception {
        // 发送一条下一分钟数据
        StockData value = lastDataState.value();
        if (value != null) {
            value.setTimestamp(value.getTimestamp() + 80 * 1000L);
            value.setState(-1);
            out.collect(value);
        }
    }
}
