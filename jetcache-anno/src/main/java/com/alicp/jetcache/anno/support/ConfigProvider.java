package com.alicp.jetcache.anno.support;

import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.CacheManager;
import com.alicp.jetcache.embedded.EmbeddedCacheBuilder;
import com.alicp.jetcache.external.ExternalCacheBuilder;
import com.alicp.jetcache.support.AbstractLifecycle;
import com.alicp.jetcache.support.StatInfo;
import com.alicp.jetcache.support.StatInfoLogger;
import com.alicp.jetcache.template.CacheBuilderTemplate;
import com.alicp.jetcache.template.CacheMonitorInstaller;
import com.alicp.jetcache.template.MetricsMonitorInstaller;
import com.alicp.jetcache.template.NotifyMonitorInstaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

// 配置提供者对象, 继承了AbstractLifecycle，走doInit
public class ConfigProvider extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ConfigProvider.class);

    /**
     * 缓存的全局配置
     */
    protected GlobalCacheConfig globalCacheConfig;

    /**
     * 根据不同类型生成缓存数据转换函数的转换器
     */
    protected EncoderParser encoderParser;

    /**
     * 根据不同类型生成缓存 Key 转换函数的转换器
     */
    protected KeyConvertorParser keyConvertorParser;
    private Consumer<StatInfo> metricsCallback;

    private CacheBuilderTemplate cacheBuilderTemplate;

    public ConfigProvider() {
        encoderParser = new DefaultEncoderParser();
        keyConvertorParser = new DefaultKeyConvertorParser();
        metricsCallback = new StatInfoLogger(false);
    }

    /**
     * 先创建cacheBuilderTemplate，之后根据globalCacheConfig.getLocalCacheBuilders()去设置setKeyConvertor，
     * 根据globalCacheConfig.getRemoteCacheBuilders()设置setKeyConvertor、setValueEncoder、setValueDecoder，
     * 最后执行initCacheMonitorInstallers方法
     */
    @Override
    protected void doInit() {
        cacheBuilderTemplate = new CacheBuilderTemplate(globalCacheConfig.isPenetrationProtect(),
                globalCacheConfig.getLocalCacheBuilders(), globalCacheConfig.getRemoteCacheBuilders());
        for (CacheBuilder builder : globalCacheConfig.getLocalCacheBuilders().values()) {
            EmbeddedCacheBuilder eb = (EmbeddedCacheBuilder) builder;
            if (eb.getConfig().getKeyConvertor() instanceof ParserFunction) {
                ParserFunction f = (ParserFunction) eb.getConfig().getKeyConvertor();
                eb.setKeyConvertor(parseKeyConvertor(f.getValue()));
            }
        }
        for (CacheBuilder builder : globalCacheConfig.getRemoteCacheBuilders().values()) {
            ExternalCacheBuilder eb = (ExternalCacheBuilder) builder;
            if (eb.getConfig().getKeyConvertor() instanceof ParserFunction) {
                ParserFunction f = (ParserFunction) eb.getConfig().getKeyConvertor();
                eb.setKeyConvertor(parseKeyConvertor(f.getValue()));
            }
            if (eb.getConfig().getValueEncoder() instanceof ParserFunction) {
                ParserFunction f = (ParserFunction) eb.getConfig().getValueEncoder();
                eb.setValueEncoder(parseValueEncoder(f.getValue()));
            }
            if (eb.getConfig().getValueDecoder() instanceof ParserFunction) {
                ParserFunction f = (ParserFunction) eb.getConfig().getValueDecoder();
                eb.setValueDecoder(parseValueDecoder(f.getValue()));
            }
        }
        // 启动缓存指标监控器，周期性打印各项指标
        initCacheMonitorInstallers();
    }

    /**
     * 会执行metricsMonitorInstaller、notifyMonitorInstaller并添加到cacheBuilderTemplate.getCacheMonitorInstallers()，
     * 最后遍历cacheBuilderTemplate.getCacheMonitorInstallers()对于是AbstractLifecycle类型的挨个执行init方法
     * metricsMonitorInstaller方法创建MetricsMonitorInstaller并执行其init方法
     */
    protected void initCacheMonitorInstallers() {
        cacheBuilderTemplate.getCacheMonitorInstallers().add(metricsMonitorInstaller());
        cacheBuilderTemplate.getCacheMonitorInstallers().add(notifyMonitorInstaller());
        for (CacheMonitorInstaller i : cacheBuilderTemplate.getCacheMonitorInstallers()) {
            // NotifyMonitorInstaller 没有实现 AbstractLifecycle 接口
            // metricsMonitorInstaller 做了 init
            if (i instanceof AbstractLifecycle) {
                ((AbstractLifecycle) i).init();
            }
        }
    }

    protected CacheMonitorInstaller metricsMonitorInstaller() {
        Duration interval = null;
        if (globalCacheConfig.getStatIntervalMinutes() > 0) {
            interval = Duration.ofMinutes(globalCacheConfig.getStatIntervalMinutes());
        }

        MetricsMonitorInstaller i = new MetricsMonitorInstaller(metricsCallback, interval);
        i.init();
        return i;
    }

    protected CacheMonitorInstaller notifyMonitorInstaller() {
        return new NotifyMonitorInstaller(area -> globalCacheConfig.getRemoteCacheBuilders().get(area));
    }

    public CacheBuilderTemplate getCacheBuilderTemplate() {
        return cacheBuilderTemplate;
    }

    @Override
    public void doShutdown() {
        try {
            for (CacheMonitorInstaller i : cacheBuilderTemplate.getCacheMonitorInstallers()) {
                if (i instanceof AbstractLifecycle) {
                    ((AbstractLifecycle) i).shutdown();
                }
            }
        } catch (Exception e) {
            logger.error("close fail", e);
        }
    }

    /**
     * Keep this method for backward compatibility.
     * NOTICE: there is no getter for encoderParser.
     */
    public Function<Object, byte[]> parseValueEncoder(String valueEncoder) {
        return encoderParser.parseEncoder(valueEncoder);
    }

    /**
     * 根据解码类型通过缓存value转换器生成解码函数
     *
     * @param valueDecoder 解码类型
     * @return 解码函数
     */
    /**
     * Keep this method for backward compatibility.
     * NOTICE: there is no getter for encoderParser.
     */
    public Function<byte[], Object> parseValueDecoder(String valueDecoder) {
        return encoderParser.parseDecoder(valueDecoder);
    }

    /**
     * 根据转换类型通过缓存key转换器生成转换函数
     *
     * @param convertor 转换类型
     * @return 转换函数
     */
    /**
     * Keep this method for backward compatibility.
     * NOTICE: there is no getter for keyConvertorParser.
     */
    public Function<Object, Object> parseKeyConvertor(String convertor) {
        return keyConvertorParser.parseKeyConvertor(convertor);
    }

    public CacheNameGenerator createCacheNameGenerator(String[] hiddenPackages) {
        return new DefaultCacheNameGenerator(hiddenPackages);
    }

    public CacheContext newContext(CacheManager cacheManager) {
        return new CacheContext(cacheManager, this, globalCacheConfig);
    }

    public void setEncoderParser(EncoderParser encoderParser) {
        this.encoderParser = encoderParser;
    }

    public void setKeyConvertorParser(KeyConvertorParser keyConvertorParser) {
        this.keyConvertorParser = keyConvertorParser;
    }

    public GlobalCacheConfig getGlobalCacheConfig() {
        return globalCacheConfig;
    }

    public void setGlobalCacheConfig(GlobalCacheConfig globalCacheConfig) {
        this.globalCacheConfig = globalCacheConfig;
    }

    public void setMetricsCallback(Consumer<StatInfo> metricsCallback) {
        this.metricsCallback = metricsCallback;
    }

}
