package ink.haifeng.quotation.model.entity;

import ink.haifeng.dto.Quotation;
import ink.haifeng.quotation.model.dto.StockData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/18 15:08:11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class StockQuotation implements Quotation {
    private Long id;
    private int tradeDay;
    private int tradeTime;
    private String stockCode;
    private BigDecimal price;
    private BigDecimal gain;
    private BigDecimal change;
    private BigDecimal preClose;
    private Long volume = 0L;
    private Long totalVolume = 0L;
    private Long amount = 0L;
    private Long totalAmount = 0L;
    private Integer realTime;
    private int createTime;
    private int updateTime;


    public StockQuotation(StockData current, StockData lastMinute) {
        this.tradeDay = current.getTradeDay();
        this.tradeTime = current.minute();
        this.stockCode = current.getStockCode();
        this.price = current.getPrice();
        this.preClose = current.getPreClose();
        this.gain = current.getPrice().divide(current.getPreClose(), 6, RoundingMode.HALF_UP)
                .subtract(BigDecimal.ONE);
        this.change = current.getPrice().subtract(current.getPreClose());
        if (lastMinute != null) {
            this.volume = current.getVolume() - lastMinute.getVolume();
            this.amount = current.getAmount() - lastMinute.getAmount();
        } else {
            this.volume = current.getVolume();
            this.amount = current.getAmount();
        }
        this.totalAmount = current.getAmount();
        this.totalVolume = current.getVolume();
        this.realTime = Integer.parseInt(current.getRealtime());
        int time = (int) (System.currentTimeMillis() / 1000);
        this.createTime = time;
        this.updateTime = time;
    }


    /**
     * 成交额和成交量为0
     * @param current
     */
    public StockQuotation(StockData current) {
        this.tradeDay = current.getTradeDay();
        this.tradeTime = current.minute();
        this.stockCode = current.getStockCode();
        this.price = current.getPrice();
        this.preClose = current.getPreClose();
        this.gain = current.getPrice().divide(current.getPreClose(), 6, RoundingMode.HALF_UP)
                .subtract(BigDecimal.ONE);
        this.change = current.getPrice().subtract(current.getPreClose());
        this.totalAmount = current.getAmount();
        this.totalVolume = current.getVolume();
        this.realTime = Integer.parseInt(current.getRealtime());
        int time = (int) (System.currentTimeMillis() / 1000);
        this.createTime = time;
        this.updateTime = time;
    }

}
