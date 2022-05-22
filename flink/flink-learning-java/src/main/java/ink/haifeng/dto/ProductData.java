package ink.haifeng.dto;


import ink.haifeng.quotation.model.dto.StockData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductData {
    private StockData current;
    private StockData lastMinute;
    private ProductData constituentsInfo;
}
