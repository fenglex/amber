package ink.haifeng.quotation.model.dto;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductConstituentsInfo {
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

    private String stockCode;
    private Integer adjShare;

    /**
     * 是否是最后一条数据
     */
    private Boolean end = false;

    public long timestamp() {
        DateTime time = DateUtil.parse(this.tradeDay + "092000", "yyyyMMddHHmmss");
        return time.getTime();
    }
}
