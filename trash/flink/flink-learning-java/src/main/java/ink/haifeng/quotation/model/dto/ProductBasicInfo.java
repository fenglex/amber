package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/17 11:07:43
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductBasicInfo {
    private int tradeDay;
    private int lastTradeDay;
    private String productCode;
    private String productName;

    /**
     * 指数当日调整市值（T+1日更新）
     */
    private BigDecimal adjMktCap;
    /**
     * 上一交易日指数调整市值（分红送转配股除权后）
     */
    private BigDecimal lastAdjMktCap;
    /**
     * 指数当日除数
     */
    private BigDecimal divisor;
    /**
     * 指数上一交易日除数
     */
    private BigDecimal lastDivisor;
    private BigDecimal closePrice;
    /**
     * 指数是否有效 1：有效，0：无效
     */
    private Short valid;
}
