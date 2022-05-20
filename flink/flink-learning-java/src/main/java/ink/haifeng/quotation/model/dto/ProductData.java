package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 16:16:02
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductData {
    private StockMinuteWithPreData stockData;
    private ProductBasicInfo productBasicInfo;
    private ProductStockConstituents productStockConstituents;
    private ProductPriceLowHigh priceLowHigh;
}
