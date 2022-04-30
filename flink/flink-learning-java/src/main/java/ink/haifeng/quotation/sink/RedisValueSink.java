package ink.haifeng.quotation.sink;

import ink.haifeng.quotation.model.dto.RedisValue;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 18:25:04
 */
public class RedisValueSink implements Sink<RedisValue>{

    private final ParameterTool parameter;

    public RedisValueSink(ParameterTool parameter) {
        this.parameter = parameter;
    }

    @Override
    public SinkFunction<RedisValue> create() {
        return new RedisSink();
    }
}
