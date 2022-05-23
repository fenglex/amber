package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 * stock关联的产品数据
 * @author haifeng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockRelateProductData {
    private int tradeDay;
    private StockDataWithPre data;
    private ProductInfo productInfo;
}
