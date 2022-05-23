package ink.haifeng.quotation.model.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductMinuteData {
    private String productCode;
    private List<StockRelateProductData> data = new ArrayList<>();
}
