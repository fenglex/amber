package ink.haifeng.quotation.model.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockPreClosePrice {
    private String stockCode;
    private int tradeDay;
    private BigDecimal preClose;

    public BigDecimal getPreClose() {
        if (this.preClose == null) {
            return BigDecimal.ZERO;
        }
        return preClose;
    }
}
