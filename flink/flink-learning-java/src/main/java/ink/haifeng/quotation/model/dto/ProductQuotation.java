package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;


/**
 * @author haifeng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ProductQuotation {
    private String productCode;
    private String productName;
    private int tradeDay;
    private int tradeTime;
    private BigDecimal price;
    private BigDecimal preClose;
    private Long amount;
    private Long totalAmount;
}
