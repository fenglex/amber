package ink.haifeng.quotation.function;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.common.functions.RichFilterFunction;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 过滤数据，删除异常数据
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 13:47:19
 */
public class DataFilterFunction extends RichFilterFunction<StockData> {

    private String runDay;

    public DataFilterFunction(String runDay) {
        this.runDay = runDay;
    }

    @Override
    public boolean filter(StockData value) throws Exception {
        if (value.getPrice().compareTo(BigDecimal.ZERO) == 0) {
            return false;
        }
        if (value.getState() == -1) {
            return false;
        }
        String current = DateUtil.format(new Date(), "yyyyMMdd");
        if (StrUtil.isEmpty(runDay)) {
            return String.valueOf(value.getTradeDay()).equals(current);
        } else {
            return String.valueOf(value.getTradeDay()).equals(runDay);
        }
    }
}