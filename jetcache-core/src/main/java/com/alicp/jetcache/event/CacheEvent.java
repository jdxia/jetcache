/**
 * Created on 2017/2/22.
 */
package com.alicp.jetcache.event;

import com.alicp.jetcache.Cache;

/**
 * The CacheEvent is used in single JVM while CacheMessage used for distributed message.
 *
 * @author huangli
 */
public class CacheEvent {
    /**
     * 它有CacheGetEvent、CacheGetAllEvent、CacheLoadEvent、CacheLoadAllEvent、CachePutEvent、
     * CachePutAllEvent、CacheRemoveEvent、CacheRemoveAllEvent这几个子类
     */

    protected Cache cache;

    public CacheEvent(Cache cache) {
        this.cache = cache;
    }

    public Cache getCache() {
        return cache;
    }

}
