/**
 * Created on  13-09-04 15:34
 */
package com.alicp.jetcache.anno.support;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.CacheConfigException;
import com.alicp.jetcache.CacheManager;
import com.alicp.jetcache.anno.CacheConsts;
import com.alicp.jetcache.anno.EnableCache;
import com.alicp.jetcache.anno.method.CacheInvokeContext;
import com.alicp.jetcache.template.QuickConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

// 缓存上下文
public class CacheContext {

    private static Logger logger = LoggerFactory.getLogger(CacheContext.class);

    private static ThreadLocal<CacheThreadLocal> cacheThreadLocal = new ThreadLocal<CacheThreadLocal>() {
        @Override
        protected CacheThreadLocal initialValue() {
            return new CacheThreadLocal();
        }
    };

    /**
     * JetCache 缓存的管理器（包含很多信息）
     */
    private ConfigProvider configProvider;

    /**
     * 缓存的全局配置
     */
    private GlobalCacheConfig globalCacheConfig;

    /**
     * 缓存实例管理器
     */
    private CacheManager cacheManager;

    public CacheContext(CacheManager cacheManager, ConfigProvider configProvider, GlobalCacheConfig globalCacheConfig) {
        this.cacheManager = cacheManager;
        this.globalCacheConfig = globalCacheConfig;
        this.configProvider = configProvider;
    }

    public CacheInvokeContext createCacheInvokeContext(ConfigMap configMap) {
        // 创建一个本次调用的上下文
        CacheInvokeContext c = newCacheInvokeContext();
        // 添加一个函数，后续用于获取缓存实例
        // 根据注解配置信息获取缓存实例对象，不存在则创建并设置到缓存注解配置类中
        c.setCacheFunction((cic, cac) -> createOrGetCache(cic, cac, configMap));
        return c;
    }

    private Cache createOrGetCache(CacheInvokeContext invokeContext, CacheAnnoConfig cacheAnnoConfig, ConfigMap configMap) {
        Cache cache = cacheAnnoConfig.getCache();
        if (cache != null) {
            return cache;
        }
        if (cacheAnnoConfig instanceof CachedAnnoConfig) { // 缓存注解
            // 根据配置创建一个缓存实例对象，通过 CacheBuilder
            cache = createCacheByCachedConfig((CachedAnnoConfig) cacheAnnoConfig, invokeContext);
        } else if ((cacheAnnoConfig instanceof CacheInvalidateAnnoConfig) || (cacheAnnoConfig instanceof CacheUpdateAnnoConfig)) { // 更新/使失效缓存注解
            cache = cacheManager.getCache(cacheAnnoConfig.getArea(), cacheAnnoConfig.getName());
            if (cache == null) {
                CachedAnnoConfig cac = configMap.getByCacheName(cacheAnnoConfig.getArea(), cacheAnnoConfig.getName());
                if (cac == null) {
                    String message = "can't find cache definition with area=" + cacheAnnoConfig.getArea()
                            + " name=" + cacheAnnoConfig.getName() +
                            ", specified in " + cacheAnnoConfig.getDefineMethod();
                    CacheConfigException e = new CacheConfigException(message);
                    logger.error("Cache operation aborted because can't find cached definition", e);
                    return null;
                }
                // 根据配置创建一个缓存实例对象，通过 CacheBuilder
                cache = createCacheByCachedConfig(cac, invokeContext);
            }
        }
        cacheAnnoConfig.setCache(cache);
        return cache;
    }

    private Cache createCacheByCachedConfig(CachedAnnoConfig ac, CacheInvokeContext invokeContext) {
        // 缓存区域
        String area = ac.getArea();
        // 缓存实例名称
        String cacheName = ac.getName();
        if (CacheConsts.isUndefined(cacheName)) {  // 没有定义缓存实例名称

            // 生成缓存实例名称：类名+方法名+(参数类型)
            cacheName = configProvider.createCacheNameGenerator(invokeContext.getHiddenPackages())
                    .generateCacheName(invokeContext.getMethod(), invokeContext.getTargetObject());
        }
        // 创建缓存实例对象
        Cache cache = __createOrGetCache(ac, area, cacheName);
        return cache;
    }

    public Cache __createOrGetCache(CachedAnnoConfig cac, String area, String cacheName) {
        QuickConfig.Builder b = QuickConfig.newBuilder(area, cacheName);
        TimeUnit timeUnit = cac.getTimeUnit();
        if (cac.getExpire() > 0) {
            b.expire(Duration.ofMillis(timeUnit.toMillis(cac.getExpire())));
        }
        if (cac.getLocalExpire() > 0) {
            b.localExpire(Duration.ofMillis(timeUnit.toMillis(cac.getLocalExpire())));
        }
        if (cac.getLocalLimit() > 0) {
            b.localLimit(cac.getLocalLimit());
        }
        b.cacheType(cac.getCacheType());
        b.syncLocal(cac.isSyncLocal());
        if (!CacheConsts.isUndefined(cac.getKeyConvertor())) {
            b.keyConvertor(configProvider.parseKeyConvertor(cac.getKeyConvertor()));
        }
        if (!CacheConsts.isUndefined(cac.getSerialPolicy())) {
            b.valueEncoder(configProvider.parseValueEncoder(cac.getSerialPolicy()));
            b.valueDecoder(configProvider.parseValueDecoder(cac.getSerialPolicy()));
        }
        b.cacheNullValue(cac.isCacheNullValue());
        b.useAreaInPrefix(globalCacheConfig.isAreaInCacheName());
        PenetrationProtectConfig ppc = cac.getPenetrationProtectConfig();
        if (ppc != null) {
            b.penetrationProtect(ppc.isPenetrationProtect());
            b.penetrationProtectTimeout(ppc.getPenetrationProtectTimeout());
        }
        b.refreshPolicy(cac.getRefreshPolicy());

        // 往下
        return cacheManager.getOrCreateCache(b.build());
    }

    protected CacheInvokeContext newCacheInvokeContext() {
        return new CacheInvokeContext();
    }

    /**
     * Enable cache in current thread, for @Cached(enabled=false).
     *
     * @param callback
     * @see EnableCache
     */
    public static <T> T enableCache(Supplier<T> callback) {
        CacheThreadLocal var = cacheThreadLocal.get();
        try {
            var.setEnabledCount(var.getEnabledCount() + 1);
            return callback.get();
        } finally {
            var.setEnabledCount(var.getEnabledCount() - 1);
        }
    }

    protected static void enable() {
        CacheThreadLocal var = cacheThreadLocal.get();
        var.setEnabledCount(var.getEnabledCount() + 1);
    }

    protected static void disable() {
        CacheThreadLocal var = cacheThreadLocal.get();
        var.setEnabledCount(var.getEnabledCount() - 1);
    }

    protected static boolean isEnabled() {
        return cacheThreadLocal.get().getEnabledCount() > 0;
    }

}
