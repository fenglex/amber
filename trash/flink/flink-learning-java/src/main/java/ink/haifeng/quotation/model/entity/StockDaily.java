package ink.haifeng.quotation.model.entity;

import ink.haifeng.quotation.model.dto.StockData;
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


    public StockDaily(StockData stockData) {
        this.tradeDay = stockData.getTradeDay();
        this.stockCode = stockData.getStockCode();
        this.closePrice = stockData.getPrice();
        this.highPrice = stockData.getHigh();
        this.lowPrice = stockData.getLow();
        this.openPrice = stockData.getOpen();
        this.closePrice = stockData.getPrice();
        this.preClosePrice = stockData.getPreClose();
        this.volume = stockData.getVolume();
        this.createTime = (int) (System.currentTimeMillis() / 1000);
        this.updateTime = (int) (System.currentTimeMillis() / 1000);
    }
}