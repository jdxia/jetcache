package com.alicp.jetcache;

import com.alicp.jetcache.event.CacheEvent;

/**
 * Created on 2016/10/25.
 *
 * @author huangli
 */
@FunctionalInterface
public interface CacheMonitor {

    /**
     * 用于消费CacheEvent，它有两个实现类，分别是DefaultCacheMonitor、CacheNotifyMonitor
     */
    void afterOperation(CacheEvent event);

}
