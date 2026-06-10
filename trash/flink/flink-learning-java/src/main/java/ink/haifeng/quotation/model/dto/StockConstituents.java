package ink.haifeng.quotation.model.dto;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class StockConstituents {
    private String stockCode;
    private BigDecimal preClose;
    private ProductBasicInfo productBasicInfo;
    private ProductConstituents constituents;
    private ProductLowHighPrice priceLowHigh;
}
