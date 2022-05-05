package ink.haifeng.quotation.sink;

import com.alibaba.fastjson.JSON;
import ink.haifeng.quotation.model.dto.RedisValue;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 13:40:33
 */
public class SinkFactory {


    public static <T> T getSink(Class<T> t, ParameterTool parameter) {
        if (t == RedisValueSink.class) {
            return t.cast(new RedisValueSink());
        } else if (t == StockDailySink.class) {
            return t.cast(new StockDailySink(parameter));
        }
        throw new IllegalArgumentException("不存在的类型");
    }


}
