package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/24 17:01:31
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductPrice {
    private BigDecimal open;
    private BigDecimal low;
    private BigDecimal high;

    public void update(BigDecimal price) {
        if (open == null) {
            open = price;
            low = price;
            high = price;
        } else {
            int compare = price.compareTo(low);
            if (compare < 0) {
                low = price;
            }
            compare = price.compareTo(high);
            if (compare > 0) {
                high = price;
            }
        }
    }
}
