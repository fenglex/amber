package ink.haifeng.quotation.handler;

import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/19 18:15:30
 */
public interface NoOutputHandler<T> {
    /**
     * 处理数据
     * @param stream
     */
    void handler(T stream,Properties properties);
}
