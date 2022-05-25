package ink.haifeng.quotation.function;

import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockMinuteData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StockMinuteDataAggregateFunction implements AggregateFunction<StockData,
        List<StockData>, StockMinuteData> {
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
}
