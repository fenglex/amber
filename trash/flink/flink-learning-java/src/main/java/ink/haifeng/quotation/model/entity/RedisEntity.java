package ink.haifeng.quotation.model.entity;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 16:37:26
 */
public interface RedisEntity {

    /**
     * 生成redis存储的string
     * @return
     */
    String redisString();
}
