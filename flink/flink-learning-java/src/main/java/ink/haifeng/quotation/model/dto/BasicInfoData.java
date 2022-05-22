package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 18:46:45
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class BasicInfoData {
    private int tradeDay;
    private List<ProductInfo> infos;
}
