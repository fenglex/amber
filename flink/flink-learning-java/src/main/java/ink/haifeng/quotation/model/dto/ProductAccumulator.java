package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductAccumulator {
    private String uuid = UUID.randomUUID().toString();
    private String productCode;
    private String productName;
    private int tradeDay;
    private int tradeTime;
    private BigDecimal totalPrice = BigDecimal.ZERO;
    private BigDecimal price;
    private BigDecimal preClose;
    private Long amount = 0L;
    private Long totalAmount = 0L;
}
