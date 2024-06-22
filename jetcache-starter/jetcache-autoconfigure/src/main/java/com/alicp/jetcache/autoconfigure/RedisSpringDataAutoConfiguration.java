package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.CacheConfigException;
import com.alicp.jetcache.external.ExternalCacheBuilder;
import com.alicp.jetcache.redis.springdata.RedisSpringDataCacheBuilder;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.Map;

/**
 * Created on 2019/5/1.
 *
 * @author huangli
 */
@Configuration
@Conditional(RedisSpringDataAutoConfiguration.SpringDataRedisCondition.class)
public class RedisSpringDataAutoConfiguration {

    public static class SpringDataRedisCondition extends JetCacheCondition {
        public SpringDataRedisCondition() {
            super("redis.springdata");
        }
    }

    @Bean
    public SpringDataRedisAutoInit springDataRedisAutoInit() {
        return new SpringDataRedisAutoInit();
    }

    /**
     * SpringDataRedisAutoInit继承自抽象类ExternalCacheAutoInit，initCache方法的实现比较简单，先是获取RedisConnectionFactory的Bean，
     * 如果有多个就获取jetCache配置中connectionFactory来决定使用哪一个RedisConnectionFactory实例，创建用于创建Lettuce类型的内存缓存的创建者RedisLettuceCacheBuilder，
     * 并调用parseGeneralConfig方法解析JetCache的配置。
     * 最后将客户端和连接对象存入自动配置的自定义容器中，并返回一个包含相关配置的ExternalCacheBuilder对象
     */
    public static class SpringDataRedisAutoInit extends ExternalCacheAutoInit implements ApplicationContextAware {

        private ApplicationContext applicationContext;

        public SpringDataRedisAutoInit() {
            super("redis.springdata");
        }

        /**
         * 初始化缓存
         *
         * @param ct 配置树
         * @param cacheAreaWithPrefix 缓存区域名称（包括前缀）
         * @return 缓存构建器
         */
        @Override
        protected CacheBuilder initCache(ConfigTree ct, String cacheAreaWithPrefix) {
            // 从应用上下文中获取 RedisConnectionFactory 的 bean 映射
            Map<String, RedisConnectionFactory> beans = applicationContext.getBeansOfType(RedisConnectionFactory.class);
            // 如果 bean 映射为空或为空，抛出异常
            if (beans == null || beans.isEmpty()) {
                throw new CacheConfigException("no RedisConnectionFactory in spring context");
            }

            // 获取一个可用的 RedisConnectionFactory 实例
            RedisConnectionFactory factory = beans.values().iterator().next();

            // 如果 Spring 上下文中有多个 RedisConnectionFactory，抛出异常
            if (beans.size() > 1) {
                String connectionFactoryName = ct.getProperty("connectionFactory");

                // 如果 connectionFactoryName 为空，抛出异常
                if (connectionFactoryName == null) {
                    throw new CacheConfigException(
                            "connectionFactory is required, because there is multiple RedisConnectionFactory in Spring context");
                }

                // 检查 bean 映射中是否包含指定的 connectionFactoryName，如果不包含，抛出异常
                if (!beans.containsKey(connectionFactoryName)) {
                    throw new CacheConfigException("there is no RedisConnectionFactory named "
                            + connectionFactoryName + " in Spring context");
                }

                // 使用指定的 connectionFactoryName 获取 RedisConnectionFactory 实例
                factory = beans.get(connectionFactoryName);
            }

            // 创建一个 ExternalCacheBuilder 实例，并使用 RedisSpringDataCacheBuilder 工具类创建构建器，设置 connectionFactory
            ExternalCacheBuilder builder = RedisSpringDataCacheBuilder.createBuilder().connectionFactory(factory);

            // 解析通用配置到构建器中
            parseGeneralConfig(builder, ct);

            // 返回构建器
            return builder;
        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = applicationContext;
        }
    }
}
