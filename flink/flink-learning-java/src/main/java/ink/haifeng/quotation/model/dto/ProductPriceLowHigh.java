package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/17 16:10:18
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductPriceLowHigh {
    private String productCode;
    /**
     * 两年最高
     */
    private BigDecimal twoYearHigh;
    /**
     * 两年最低
     */
    private BigDecimal twoYearLow;
    /**
     * 五年最高
     */
    private BigDecimal fiveYearHigh;
    /**
     * 五年最低
     */
    private BigDecimal fiveYearLow;

}
