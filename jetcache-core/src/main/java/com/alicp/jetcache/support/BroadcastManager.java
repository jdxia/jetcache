/**
 * Created on 2019/6/10.
 */
package com.alicp.jetcache.support;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.CacheManager;
import com.alicp.jetcache.CacheResult;
import com.alicp.jetcache.CacheUtil;
import com.alicp.jetcache.MultiLevelCache;
import com.alicp.jetcache.embedded.AbstractEmbeddedCache;
import com.alicp.jetcache.external.ExternalCacheConfig;
import com.alicp.jetcache.CacheConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * BroadcastManager是个抽象类,
 * 实现了AutoCloseable接口，其close方法默认为空实现,
 * 定义了publish及startSubscribe两个抽象方法
 *
 * 主要有MockRemoteCacheBuilder的匿名实现、RedisBroadcastManager、LettuceBroadcastManager、SpringDataBroadcastManager、RedissonBroadcastManager这几个实现
 */
public abstract class BroadcastManager implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(BroadcastManager.class);

    private final String sourceId = UUID.randomUUID().toString();
    private final CacheManager cacheManager;

    public BroadcastManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    protected void checkConfig(ExternalCacheConfig config) {
        if (config.getBroadcastChannel() == null) {
            throw new CacheConfigException("BroadcastChannel not set");
        }
        if (config.getValueEncoder() == null) {
            throw new CacheConfigException("no value encoder");
        }
        if (config.getValueDecoder() == null) {
            throw new CacheConfigException("no value decoder");
        }
    }

    public abstract CacheResult publish(CacheMessage cacheMessage);

    public abstract void startSubscribe();

    @Override
    public void close() throws Exception {
    }

    public String getSourceId() {
        return sourceId;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    protected void processNotification(byte[] message, Function<byte[], Object> decoder) {
        try {
            if (message == null) {
                logger.error("notify message is null");
                return;
            }
            Object value = decoder.apply(message);
            if (value == null) {
                logger.error("notify message is null");
                return;
            }
            if (value instanceof CacheMessage) {
                // 核心
                processCacheMessage((CacheMessage) value);
            } else {
                logger.error("the message is not instance of CacheMessage, class={}", value.getClass());
            }
        } catch (Throwable e) {
            SquashedLogger.getLogger(logger).error("receive cache notify error", e);
        }
    }

    private void processCacheMessage(CacheMessage cacheMessage) {
        // 看是不是自己本机, 本机就忽略, 不处理这个消息
        if (sourceId.equals(cacheMessage.getSourceId())) {
            return;
        }
        Cache cache = cacheManager.getCache(cacheMessage.getArea(), cacheMessage.getCacheName());
        if (cache == null) {
            logger.warn("Cache instance not exists: {},{}", cacheMessage.getArea(), cacheMessage.getCacheName());
            return;
        }
        Cache absCache = CacheUtil.getAbstractCache(cache);
        //如果不是多级缓存就忽略
        if (!(absCache instanceof MultiLevelCache)) {
            logger.warn("Cache instance is not MultiLevelCache: {},{}", cacheMessage.getArea(), cacheMessage.getCacheName());
            return;
        }
        Cache[] caches = ((MultiLevelCache) absCache).caches();
        Set<Object> keys = Stream.of(cacheMessage.getKeys()).collect(Collectors.toSet());
        // 多级缓存, 这边有本地和远程的
        for (Cache c : caches) {
            Cache localCache = CacheUtil.getAbstractCache(c);
            // 只处理本地缓存的
            if (localCache instanceof AbstractEmbeddedCache) {
                ((AbstractEmbeddedCache) localCache).__removeAll(keys);
            } else {
                break;
            }
        }
    }

}
