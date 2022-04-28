package ink.haifeng.quotation.sink;

import cn.hutool.core.map.MapUtil;
import cn.hutool.crypto.SecureUtil;
import ink.haifeng.quotation.model.dto.RedisValue;
import ink.haifeng.quotation.model.dto.StockData;

import ink.haifeng.quotation.model.entity.RedisEntity;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 16:36:47
 */
public class RedisSink extends RichSinkFunction<RedisValue> {
    private JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
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
    public void invoke(RedisValue value, Context context) throws Exception {
        String stockKey = String.format("%s%s%s", "69", "901", SecureUtil.md5("").substring(8, 24));
        String productKey = String.format("%s%s%s", "69", "902", SecureUtil.md5("").substring(8, 24));
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.hmset(value.getKey(), MapUtil.of(value.getHashKey(), value.getHashValue()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        jedisPool.close();
    }
}
