package ink.haifeng.quotation.model.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * @author haifeng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockMinuteData {
    private int tradeDay;
    private int tradeTime;
    private Map<String,StockData> minuteDataMap;

}
