package com.alicp.jetcache.redisson;

import com.alicp.jetcache.external.ExternalCacheConfig;
import com.alicp.jetcache.mq.JetCacheMQConsumerStarter;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.redisson.api.RedissonClient;

/**
 * Created on 2022/7/12.
 *
 * @author <a href="mailto:jeason1914@qq.com">yangyong</a>
 */
public class RedissonCacheConfig<K, V> extends ExternalCacheConfig<K, V> {
    private RedissonClient redissonClient;

    private RocketMQClientTemplate rocketMQClientTemplate;

    private JetCacheMQConsumerStarter mqConsumerStarter;


    private String mqTopic;

    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    public void setRedissonClient(final RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    public RocketMQClientTemplate getRocketMQClientTemplate() {
        return rocketMQClientTemplate;
    }

    public void setRocketMQClientTemplate(RocketMQClientTemplate rocketMQClientTemplate) {
        this.rocketMQClientTemplate = rocketMQClientTemplate;
    }

    public String getMqTopic() {
        return mqTopic;
    }

    public void setMqTopic(String mqTopic) {
        this.mqTopic = mqTopic;
    }

    public void setMqConsumerStarter(JetCacheMQConsumerStarter mqConsumerStarter) {
        this.mqConsumerStarter = mqConsumerStarter;
    }

    public JetCacheMQConsumerStarter getMqConsumerStarter() {
        return mqConsumerStarter;
    }
}
