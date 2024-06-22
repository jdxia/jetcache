package com.alicp.jetcache;

/**
 * Created on 2016/11/17.
 *
 * @author huangli
 */
public interface CacheBuilder {
    // 只定义了一个buildCache()方法，用于构建缓存实例，交由不同的实现类
    <K, V> Cache<K, V> buildCache();
}
