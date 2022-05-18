package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 个股分钟数据，包含前一分钟的数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockMinuteWithPreData {
    private StockData current;
    private StockData lastMinute;
}
