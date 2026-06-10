package ink.haifeng.sink;

import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.dto.StockDataWithPre;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/18 17:52:53
 */
public class StockAggregate extends RichAggregateFunction<StockData, StockData, StockDataWithPre> {


    @Override
    public StockData createAccumulator() {
        return new StockData();
    }

    @Override
    public StockData add(StockData value, StockData accumulator) {
        return value.getTimestamp() > accumulator.getTimestamp() ? value : accumulator;
    }

    @SneakyThrows
    @Override
    public StockDataWithPre getResult(StockData accumulator) {
        if (lastDataState.value() == null) {
            lastDataState.update(accumulator);
            return new StockDataWithPre(accumulator, null);
        } else {
            if (lastDataState.value().getTradeDay() == accumulator.getTradeDay()) {
                StockDataWithPre result = new StockDataWithPre(accumulator, lastDataState.value());
                lastDataState.update(accumulator);
                return result;
            } else {
                return new StockDataWithPre(accumulator, null);
            }
        }
    }

    @Override
    public StockData merge(StockData a, StockData b) {
        return a.getTimestamp() > b.getTimestamp() ? a : b;
    }

    private ValueState<StockData> lastDataState;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastDataState = this.getRuntimeContext().getState(new ValueStateDescriptor<StockData>("last-data",
                StockData.class));
    }
}
