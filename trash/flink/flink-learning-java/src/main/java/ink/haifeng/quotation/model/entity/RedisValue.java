package ink.haifeng.quotation.model.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 17:55:34
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RedisValue {
    private String key;
    private String hashKey;
    private String hashValue;
}
