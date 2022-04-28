package ink.haifeng.quotation.model.entity;

import ink.haifeng.quotation.model.dto.StockData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 18:06:09
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockDataAndConstituent {
    private StockData data;
    private ProductIndexConstituents constituents;
}
