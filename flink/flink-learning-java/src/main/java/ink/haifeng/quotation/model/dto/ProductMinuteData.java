package ink.haifeng.quotation.model.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author haifeng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductMinuteData {
    private StockMinuteWithPreData data;
    private ProductInfo productInfo;
}
