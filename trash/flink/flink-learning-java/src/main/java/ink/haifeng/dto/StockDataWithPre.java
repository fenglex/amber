package ink.haifeng.dto;

import ink.haifeng.quotation.model.dto.StockData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/18 17:45:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class StockDataWithPre {
    private StockData current;
    private StockData lastMinute;
}
