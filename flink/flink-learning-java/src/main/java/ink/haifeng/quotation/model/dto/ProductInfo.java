package ink.haifeng.quotation.model.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductInfo {

    public ProductInfo(String productCode) {
        this.productCode = productCode;
    }

    private String productCode;
    /**
     * 产品基础信息
     */
    private ProductBasicInfo basicInfo;
    /**
     * 产品成分股
     */
    private List<ProductConstituents> constituents = new ArrayList<>();
    /**
     * 产品历史最高最低价
     */
    private ProductLowHighPrice lowHighPrice;
    /**
     * 产品成分股前日收盘价
     */
    private Map<String, StockPreClosePrice> stockPreClose = new HashMap<>();
}
