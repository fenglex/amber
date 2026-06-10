package ink.haifeng.quotation;

import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 11:15:27
 */
public class StockMinuteMapFunction extends RichMapFunction<StockData,StockData> {
    @Override
    public StockData map(StockData value) throws Exception {
        return null;
    }
}
