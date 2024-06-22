package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.SimpleCacheManager;
import com.alicp.jetcache.anno.support.EncoderParser;
import com.alicp.jetcache.anno.support.GlobalCacheConfig;
import com.alicp.jetcache.anno.support.JetCacheBaseBeans;
import com.alicp.jetcache.anno.support.KeyConvertorParser;
import com.alicp.jetcache.anno.support.SpringConfigProvider;
import com.alicp.jetcache.mq.JetCacheMQConsumerStarter;
import com.alicp.jetcache.support.StatInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.function.Consumer;

/**
 * spring 自动装配的
 *
 * 该 Bean 将会被 Spring 容器注入，依次注入下面几个 Bean
 * SpringConfigProvider -> AutoConfigureBeans -> BeanDependencyManager(为 GlobalCacheConfig 添加 CacheAutoInit 依赖) -> GlobalCacheConfig
 * 由此会完成初始化配置操作，缓存实例构造器 CacheBuilder 也会被注入容器
 */
@Configuration
@ConditionalOnClass(GlobalCacheConfig.class)
// globalCacheConfig 设置全局的相关配置并添加已经初始化的CacheBuilder构造器，然后返回GlobalCacheConfig让Spring容器管理，这样一来就完成了JetCache的解析配置并初始化的功能
@ConditionalOnMissingBean(GlobalCacheConfig.class)
@EnableConfigurationProperties(JetCacheProperties.class)
@Import({RedisAutoConfiguration.class,
        CaffeineAutoConfiguration.class,
        MockRemoteCacheAutoConfiguration.class,
        LinkedHashMapAutoConfiguration.class,
        RedisLettuceAutoConfiguration.class,
        RedisSpringDataAutoConfiguration.class,
        RedissonAutoConfiguration.class,
        JetCacheMQConsumerStarter.class})
public class JetCacheAutoConfiguration {

    public static final String GLOBAL_CACHE_CONFIG_NAME = "globalCacheConfig";

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public SpringConfigProvider springConfigProvider(
            @Autowired ApplicationContext applicationContext,
            @Autowired GlobalCacheConfig globalCacheConfig,
            @Autowired(required = false) EncoderParser encoderParser,
            @Autowired(required = false) KeyConvertorParser keyConvertorParser,
            @Autowired(required = false) Consumer<StatInfo> metricsCallback) {
        // 执行 springConfigProvider 方法
        return new JetCacheBaseBeans().springConfigProvider(applicationContext, globalCacheConfig,
                encoderParser, keyConvertorParser, metricsCallback);
    }

    @Bean(name = "jcCacheManager",destroyMethod = "close")
    @ConditionalOnMissingBean
    public SimpleCacheManager cacheManager(@Autowired SpringConfigProvider springConfigProvider) {
        SimpleCacheManager cacheManager = new SimpleCacheManager();
        cacheManager.setCacheBuilderTemplate(springConfigProvider.getCacheBuilderTemplate());
        return cacheManager;
    }

    @Bean
    @ConditionalOnMissingBean
    public AutoConfigureBeans autoConfigureBeans() {
        return new AutoConfigureBeans();
    }

    @Bean
    public static BeanDependencyManager beanDependencyManager() {
        return new BeanDependencyManager();
    }

    @Bean(name = GLOBAL_CACHE_CONFIG_NAME)
    public GlobalCacheConfig globalCacheConfig(AutoConfigureBeans autoConfigureBeans, JetCacheProperties props) {
        GlobalCacheConfig _globalCacheConfig = new GlobalCacheConfig();
        _globalCacheConfig.setHiddenPackages(props.getHiddenPackages());
        _globalCacheConfig.setStatIntervalMinutes(props.getStatIntervalMinutes());
        _globalCacheConfig.setAreaInCacheName(props.isAreaInCacheName());
        _globalCacheConfig.setPenetrationProtect(props.isPenetrationProtect());
        _globalCacheConfig.setEnableMethodCache(props.isEnableMethodCache());
        _globalCacheConfig.setLocalCacheBuilders(autoConfigureBeans.getLocalCacheBuilders());
        _globalCacheConfig.setRemoteCacheBuilders(autoConfigureBeans.getRemoteCacheBuilders());
        return _globalCacheConfig;
    }

}
