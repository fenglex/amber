package ink.haifeng.quotation.model.dto;

import ink.haifeng.quotation.model.entity.ProductIndexConstituents;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 18:14:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StringConstituent {
    private String stock;
    private ProductIndexConstituents constituents;
}
