package ink.haifeng.quotation.model.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/23 17:20:50
 */
@Data
public class ProductData {
    private StockDataWithPre stockData;
    private String productCode;
    private ProductBasicInfo basicInfo;
    private List<ProductConstituents> constituents;
    private Map<String, StockPreClosePrice> stockPreClose;
}
