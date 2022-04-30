package ink.haifeng.quotation.sink;

import ink.haifeng.quotation.model.dto.RedisValue;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 13:40:33
 */
public class SinkFactory {
    private final static ParameterTool parameter;

    public SinkFactory(ParameterTool parameter) {
        this.parameter = parameter;
    }

    public  SinkFunction<?> getSink(Class clazz) {
        if (t instanceof RedisValue ) {
            return new RedisValueSink(parameter);
        } else {
            throw new IllegalArgumentException("sink not exists");
        }
    }
}
