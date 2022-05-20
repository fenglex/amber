package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 18:46:45
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductBroadcastData {
    private int tradeDay;
    private List<ProductInfo> products;
}
