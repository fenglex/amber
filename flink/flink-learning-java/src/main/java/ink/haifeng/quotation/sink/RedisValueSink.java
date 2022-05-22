package ink.haifeng.quotation.sink;

import cn.hutool.core.map.MapUtil;
import ink.haifeng.quotation.model.entity.RedisValue;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 18:25:04
 */
public class RedisValueSink implements Sink<RedisValue>{

    @Override
    public SinkFunction<RedisValue> sink() {
        return new RichSinkFunction<RedisValue>() {
            private JedisPool jedisPool;

            @Override
            public void open(Configuration parameters) {
                ExecutionConfig.GlobalJobParameters jobParameters =
                        this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                ParameterTool parameterTool = (ParameterTool) jobParameters;
                String redisHost = parameterTool.get("redis.host");
                int redisPort = parameterTool.getInt("redis.port");
                String redisPassword = parameterTool.get("redis.password");
                int redisDatabase = parameterTool.getInt("redis.database");
                JedisPoolConfig poolConfig = new JedisPoolConfig();
                poolConfig.setMaxTotal(10);
                poolConfig.setMaxIdle(5);
                poolConfig.setMaxWait(Duration.ofSeconds(10));
                poolConfig.setTestOnBorrow(true);
                jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 6 * 1000, redisPassword, redisDatabase);

            }

            @Override
            public void invoke(RedisValue value, Context context) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.hmset(value.getKey(), MapUtil.of(value.getHashKey(), value.getHashValue()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void close() throws Exception {
                jedisPool.close();
            }
        };
    }



}


