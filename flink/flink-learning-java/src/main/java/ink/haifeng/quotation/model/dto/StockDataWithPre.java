package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 个股分钟数据，包含前一分钟的数据
 * @author haifeng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockDataWithPre {
    /**
     * 当前分钟数据
     */
    private StockData current;
    /**
     * 前一分钟数据
     */
    private StockData lastMinute;
}
