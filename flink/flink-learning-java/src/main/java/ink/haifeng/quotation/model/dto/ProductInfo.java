package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/17 11:01:11
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ProductInfo {
    private ProductBasicInfo basicInfo;
    private List<ProductStockConstituents> constituents = new ArrayList<>();
    private ProductPriceLowHigh priceLowHigh;
}
