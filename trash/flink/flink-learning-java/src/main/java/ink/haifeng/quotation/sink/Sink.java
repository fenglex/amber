package ink.haifeng.quotation.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 18:13:38
 */
public interface Sink<IN> {

    /**
     * 创建自定义sink
     *
     * @return
     */
     SinkFunction<IN> sink();
}
