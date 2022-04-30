package ink.haifeng.quotation.model.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 18:18:12
 */
@Data
@ToString
@NoArgsConstructor
public class StockDaily {
    private long id;
    private int tradeDay;
    private String stockCode;
    private BigDecimal closePrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal openPrice;
    private BigDecimal preClosePrice;
    private Long volume;
    private int createTime;
    private int updateTime;
}