package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/17 11:03:34
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductStockConstituents {
    private String productCode;
    private String stockCode;
    private Integer adjShare;
}
