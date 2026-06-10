package ink.haifeng.quotation.source;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/18 16:14:39
 */
public interface CustomizedSource {

    /**
     * 自定义的source
     *
     * @return
     */
    Source source();
}
