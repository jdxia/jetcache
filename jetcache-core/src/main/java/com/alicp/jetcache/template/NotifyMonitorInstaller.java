/**
 * Created on 2022/08/01.
 */
package com.alicp.jetcache.template;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.CacheManager;
import com.alicp.jetcache.CacheMonitor;
import com.alicp.jetcache.CacheUtil;
import com.alicp.jetcache.MultiLevelCache;
import com.alicp.jetcache.external.AbstractExternalCache;
import com.alicp.jetcache.external.ExternalCacheBuilder;
import com.alicp.jetcache.support.BroadcastManager;
import com.alicp.jetcache.support.CacheNotifyMonitor;

import java.util.function.Function;

/**
 * @author huangli
 */
public class NotifyMonitorInstaller implements CacheMonitorInstaller {

    private final Function<String, CacheBuilder> remoteBuilderTemplate;

    public NotifyMonitorInstaller(Function<String, CacheBuilder> remoteBuilderTemplate) {
        this.remoteBuilderTemplate = remoteBuilderTemplate;
    }

    // 原来的逻辑, 用redis的pub/sub做的
    @Override
    public void addMonitors(CacheManager cacheManager, Cache cache, QuickConfig quickConfig) {
        if (quickConfig.getSyncLocal() == null || !quickConfig.getSyncLocal()) {
            return;
        }

        // 如果不是MultiLevelCache, 那就不用管了, 但是目前本地缓存需要支持
//        if (!(CacheUtil.getAbstractCache(cache) instanceof MultiLevelCache)) {
//            return;
//        }

        // 如果是外部缓存就不用管了
        if (CacheUtil.getAbstractCache(cache) instanceof AbstractExternalCache) {
            return;
        }

        String area = quickConfig.getArea(); // 默认default
        final ExternalCacheBuilder cacheBuilder = (ExternalCacheBuilder) remoteBuilderTemplate.apply(area); //如果配置的redisson, 那这就是redissonBuilder
        // 看这个cache支不支持广播, cacheBuilder不能为空, 并且要有广播channel
        if (cacheBuilder == null || !cacheBuilder.supportBroadcast()
                || cacheBuilder.getConfig().getBroadcastChannel() == null) {
            return;
        }

        if (cacheManager.getBroadcastManager(area) == null) {  // 一开始null
            /**
             * 如果用的是redisson的话, 这边会走这里创建一个BroadcastManager
             * {@link com.alicp.jetcache.redisson.RedissonCacheBuilder#createBroadcastManager(CacheManager)}
             */
            BroadcastManager cm = cacheBuilder.createBroadcastManager(cacheManager);
            if (cm != null) { // 不是null了
                cm.startSubscribe();
                cacheManager.putBroadcastManager(area, cm);
            }
        }

        CacheMonitor monitor = new CacheNotifyMonitor(cacheManager, area, quickConfig.getName());
        cache.config().getMonitors().add(monitor);
    }
}
