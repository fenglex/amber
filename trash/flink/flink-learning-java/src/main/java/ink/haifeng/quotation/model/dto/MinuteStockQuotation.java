package ink.haifeng.quotation.model.dto;

import ink.haifeng.quotation.model.entity.StockQuotation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MinuteStockQuotation {
    private int tradeDay;
    private int tradeTime;
    private Map<String,StockQuotation> quotationMap;
}
