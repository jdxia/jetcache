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

    /**
     * 仅当 quickConfig.syncLocal = true 且远端缓存 builder 支持广播（如 Redisson）时：
     * 启动或复用 BroadcastManager 的订阅；
     * 给当前 Cache 添加 CacheNotifyMonitor，实现本地缓存协同失效/更新。
     */
    @Override
    public void addMonitors(CacheManager cacheManager, Cache cache, QuickConfig quickConfig) {
        // syncLocal未启用则直接返回
        if (quickConfig.getSyncLocal() == null || !quickConfig.getSyncLocal()) {
            return;
        }
        if (!(CacheUtil.getAbstractCache(cache) instanceof MultiLevelCache)) {
            return;
        }
        // 默认default
        String area = quickConfig.getArea();

        //如果配置的redisson, 那这就是redissonBuilder
        final ExternalCacheBuilder cacheBuilder = (ExternalCacheBuilder) remoteBuilderTemplate.apply(area);
        // 看这个cache支不支持广播, cacheBuilder不能为空, 并且要有广播channel
        if (cacheBuilder == null || !cacheBuilder.supportBroadcast()
                || cacheBuilder.getConfig().getBroadcastChannel() == null) {
            return;
        }

        // 一开始null
        if (cacheManager.getBroadcastManager(area) == null) {
            /**
             * 如果用的是redission的话, 这边会走这里创建一个BroadcastManager
             * {@link com.alicp.jetcache.redisson.RedissonCacheBuilder#createBroadcastManager(CacheManager)}
             */
            BroadcastManager cm = cacheBuilder.createBroadcastManager(cacheManager);
            // 不是null了
            if (cm != null) {
                // 核心是这里
                cm.startSubscribe();
                cacheManager.putBroadcastManager(area, cm);
            }
        }

        // 添加CacheNotifyMonitor监听器
        CacheMonitor monitor = new CacheNotifyMonitor(cacheManager, area, quickConfig.getName());
        cache.config().getMonitors().add(monitor);
    }
}
