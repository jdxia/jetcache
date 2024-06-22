package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.CacheConfigException;
import com.alicp.jetcache.external.ExternalCacheBuilder;
import com.alicp.jetcache.mq.JetCacheMQConsumerStarter;
import com.alicp.jetcache.redisson.RedissonCacheBuilder;
import jakarta.annotation.PostConstruct;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.redisson.api.RedissonClient;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Objects;

/**
 * 当spring boot项目的application.yml文件中jetcache.local.${areaName} .type或jetcache.remote.${areaName}.type为redisson时，就会构建RedissonAutoConfiguration对应的bean
 */
@Configuration
@Conditional(RedissonAutoConfiguration.RedissonCondition.class)
public class RedissonAutoConfiguration {
    private static final String CACHE_TYPE = "redisson";

    private static String mqTopic;

    @Value("${jetcache.mqTopic:jetcache-topic}")
    private String topicName;

    @PostConstruct
    public void init() {
        mqTopic = topicName;
    }

    public static class RedissonCondition extends JetCacheCondition {
        public RedissonCondition() {
            super(CACHE_TYPE);
        }
    }

    @Bean
    public RedissonAutoInit redissonAutoInit() {
        return new RedissonAutoInit();
    }

    public static class RedissonAutoInit extends ExternalCacheAutoInit implements ApplicationContextAware {
        private ApplicationContext context;

        public RedissonAutoInit() {
            super(CACHE_TYPE);
        }

        /**
         * 初始化缓存
         *
         * @param ct            缓存配置树
         * @param cacheAreaWithPrefix 缓存区域名称（包括前缀）
         * @return 缓存构建器
         *
         * 获取所有RedissonClient的bean映射，如果找到了多个RedissonClient的bean，则根据配置决定使用哪个RedissonClient。
         * 创建一个ExternalCacheBuilder对象，并将RedissonClient设置为其属性。
         * 解析通用配置并添加到CacheBuilder中
         */
        @Override
        protected CacheBuilder initCache(final ConfigTree ct, final String cacheAreaWithPrefix) {
            // 获取RedissonClient的bean
            final Map<String, RedissonClient> beans = this.context.getBeansOfType(RedissonClient.class);
            if (beans.isEmpty()) {
                throw new CacheConfigException("no RedissonClient in spring context");
            }
            RedissonClient client = beans.values().iterator().next();
            if (beans.size() > 1) {
                // 获取配置树中的redissonClient属性值
                final String redissonClientName = ct.getProperty("redissonClient");
                if (Objects.isNull(redissonClientName) || redissonClientName.isEmpty()) {
                    throw new CacheConfigException("redissonClient is required, because there is multiple RedissonClient in Spring context");
                }
                if (!beans.containsKey(redissonClientName)) {
                    throw new CacheConfigException("there is no RedissonClient named " + redissonClientName + " in Spring context");
                }
                client = beans.get(redissonClientName);
            }

            // 获取mq的发布者客户端
            RocketMQClientTemplate mqClientTemplate = this.context.getBean(RocketMQClientTemplate.class);

            // 获取mq消费者配置
            JetCacheMQConsumerStarter mqConsumerStarter = this.context.getBean(JetCacheMQConsumerStarter.class);

            // 创建外部缓存构建器并设置RedissonClient
            final ExternalCacheBuilder<?> builder = RedissonCacheBuilder.createBuilder()
                    .redissonClient(client)
                    .setRocketMQClientTemplate(mqClientTemplate)
                    .setMQConsumerConfig(mqConsumerStarter)
                    .setMqTopic(mqTopic);

            // 解析通用配置并添加到构建器中, 重点, 里面有pubsub
            parseGeneralConfig(builder, ct);
            return builder;
        }

        @Override
        public void setApplicationContext(final ApplicationContext context) throws BeansException {
            this.context = context;
        }
    }
}
