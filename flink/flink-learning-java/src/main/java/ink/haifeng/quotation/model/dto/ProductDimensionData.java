package ink.haifeng.quotation.model.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/17 11:01:11
 */
@Data
public class ProductDimensionData {
    private String productCode;
    private String productName;
    private int tradeDay;
    private int lastTradeDay;
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
    /**
     * 前日收盘价
     */
    private BigDecimal closePrice;
    /**
     * 指数是否有效 1：有效，0：无效
     */
    private Short valid;


    /**
     * 每个个股的贡献度
     */
    private List<ProductStockConstituents> constituents=new ArrayList<>();


    /**
     * 两年最高
     */
    private BigDecimal twoYearHigh;
    /**
     * 两年最低
     */
    private BigDecimal twoYearLow;
    /**
     * 五年最高
     */
    private BigDecimal fiveYearHigh;
    /**
     * 五年最低
     */
    private BigDecimal fiveYearLow;


    public ProductDimensionData(ProductBasicInfo basicInfo) {
        this.tradeDay = basicInfo.getTradeDay();
        this.lastTradeDay = basicInfo.getLastTradeDay();
        this.productCode = basicInfo.getProductCode();
        this.productName = basicInfo.getProductName();
        this.adjMktCap = basicInfo.getAdjMktCap();
        this.lastAdjMktCap = basicInfo.getLastAdjMktCap();
        this.divisor = basicInfo.getDivisor();
        this.lastDivisor = basicInfo.getLastDivisor();
        this.closePrice = basicInfo.getClosePrice();
        this.valid = basicInfo.getValid();
    }
}
