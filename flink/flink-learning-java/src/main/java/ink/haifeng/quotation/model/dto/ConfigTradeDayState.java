package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConfigTradeDayState {
    private Integer tradeDay;
    private Boolean isTradeDay;
}
