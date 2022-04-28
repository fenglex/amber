package ink.haifeng.quotation.model.entity;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 15:38:00
 */
@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductIndexConstituents {

    private int tradeDay;
    private String productCode;
    private String stockCode;
    /**
     * 调整股本数
     */
    private int adjShare;

    public long timestamp() {
        DateTime time = DateUtil.parse( this.tradeDay + "092000","yyyyMMddHHmmss");
        return time.getTime();
    }
}
