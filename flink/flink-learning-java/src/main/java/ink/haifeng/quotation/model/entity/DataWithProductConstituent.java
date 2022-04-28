package ink.haifeng.quotation.model.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 17:25:42
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class DataWithProductConstituent {
    private ProductIndexBasicInfo basicInfo;
    private List<ProductIndexConstituents> constituents = new ArrayList<>(20);

    private StockQuotation data;
}
