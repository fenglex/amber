package ink.haifeng.quotation.handler;

import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 10:59:08
 */
public interface WithOutputHandler<IN, OUT> {

    /**
     * 进行流转换
     *
     * @param in
     * @return
     */
    OUT handler(IN in,Properties properties);
}
