package ink.haifeng.quotation.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 14:01:16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserTimer {
    private String uid;
    private long time;
}
