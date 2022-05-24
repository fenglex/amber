package ink.haifeng.quotation.model.entity;

import ink.haifeng.quotation.model.dto.ProductBasicInfo;
import ink.haifeng.quotation.model.dto.ProductPrice;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/24 17:35:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductEod {
    private String productCode;
    private String productName;
    private int tradeDay;
    private BigDecimal closePrice;
    private BigDecimal highPrice;
    private BigDecimal openPrice;
    private BigDecimal lowPrice;
    private BigDecimal preClosePrice;
    private BigDecimal adjFactor;
    private long amount;

    public ProductEod(ProductBasicInfo basicInfo, long amount, BigDecimal closePrice, ProductPrice productPrice) {
        this.productCode = basicInfo.getProductCode();
        this.productName = basicInfo.getProductName();
        this.tradeDay = basicInfo.getTradeDay();
        if (closePrice != null && closePrice.compareTo(BigDecimal.ZERO) != 0) {
            this.closePrice = closePrice;
        } else {
            this.closePrice = basicInfo.getClosePrice();
        }
        if (productPrice != null && productPrice.getOpen() != null) {
            this.openPrice = productPrice.getOpen();
        } else {
            this.openPrice = basicInfo.getClosePrice();
        }
        if (productPrice != null && productPrice.getHigh() != null) {
            this.highPrice = productPrice.getHigh();
        } else {
            this.highPrice = basicInfo.getClosePrice();
        }
        if (productPrice != null && productPrice.getLow() != null) {
            this.lowPrice = productPrice.getLow();
        } else {
            this.lowPrice = basicInfo.getClosePrice();
        }
        this.preClosePrice = basicInfo.getClosePrice();
        this.adjFactor = new BigDecimal("1");
        this.amount = amount;
    }
}
