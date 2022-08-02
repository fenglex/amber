package ink.haifeng.quotation.function;

import ink.haifeng.quotation.model.dto.MinuteStockQuotation;
import ink.haifeng.quotation.model.dto.StockMinuteData;
import ink.haifeng.quotation.model.entity.StockQuotation;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 合并分钟数据
 */
public class StockMinuteQuotationAggregateFunction
        implements AggregateFunction<StockQuotation, List<StockQuotation>, MinuteStockQuotation> {
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
        a.addAll(b);
        return a;
    }
}
