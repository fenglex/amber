package ink.haifeng.quotation.sink;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 13:40:33
 */
public class SinkFactory {


    public static <T> T getSink(Class<T> t, Properties properties) {
        if (t == RedisValueSink.class) {
            return t.cast(new RedisValueSink());
        } else if (t == StockDailySink.class) {
            return t.cast(new StockDailySink(properties));
        }
        throw new IllegalArgumentException("不存在的类型");
    }


}
