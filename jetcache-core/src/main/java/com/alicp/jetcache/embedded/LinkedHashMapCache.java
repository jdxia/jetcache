/**
 * Created on  13-09-12 19:02
 */
package com.alicp.jetcache.embedded;

import com.alicp.jetcache.CacheResultCode;
import com.alicp.jetcache.CacheValueHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author huangli
 */
// LinkedHashMapCache自定义LRUMap继承LinkedHashMap并实现InnerMap接口
public class LinkedHashMapCache<K, V> extends AbstractEmbeddedCache<K, V> {

    private static Logger logger = LoggerFactory.getLogger(LinkedHashMapCache.class);

    public LinkedHashMapCache(EmbeddedCacheConfig<K, V> config) {
        super(config);
        // 将缓存实例添加至 Cleaner
        // 对象初始化时会被添加至com.alicp.jetcache.embedded.Cleaner清理器中，Cleaner会周期性（每隔60秒）遍历LinkedHashMapCache缓存实例，调用其cleanExpiredEntry方法
        addToCleaner();
    }

    protected void addToCleaner() {
        Cleaner.add(this);
    }

    @Override
    protected InnerMap createAreaCache() {
        return new LRUMap(config.getLimit());
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.equals(LinkedHashMap.class)) {
            return (T) innerMap;
        }
        throw new IllegalArgumentException(clazz.getName());
    }

    public void cleanExpiredEntry() {
        ((LRUMap) innerMap).cleanExpiredEntry();
    }

    /**
     * 用于本地缓存类型为 linkedhashmap 缓存实例存储缓存数据
     */
    final class LRUMap extends LinkedHashMap implements InnerMap {

        /**
         * 允许的最大缓存数量
         */
        private final int max;
//        private final Object lockObj;

        /**
         * 缓存实例锁
         */
        private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

        public LRUMap(int max) {
            // 自定义max字段，存储元素个数的最大值，并设置初始容量为(max * 1.4f)
            super((int) (max * 1.4f), 0.75f, true);
            this.max = max;
//            this.lockObj = lockObj;
        }

        /**
         * 当元素大于最大值时移除最老的元素
         *
         * @param eldest 最老的元素
         * @return 是否删除
         */
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > max;
        }

        /**
         * 清理过期的元素
         * 遍历Map，根据缓存value（被封装成的 com.alicp.jetcache.CacheValueHolder 对象，包含缓存数据、失效时间戳和第一次访问的时间），
         * 清理过期的元素
         */
        void cleanExpiredEntry() {
            Lock lock = readWriteLock.writeLock();
            lock.lock(); // 占有当前缓存实例这把锁
            try{
                for (Iterator it = entrySet().iterator(); it.hasNext();) {
                    Map.Entry en = (Map.Entry) it.next();
                    Object value = en.getValue();
                    if (value != null && value instanceof CacheValueHolder) {
                        CacheValueHolder h = (CacheValueHolder) value;
                        /*
                         * 缓存的数据已经失效了则删除
                         */
                        if (System.currentTimeMillis() >= h.getExpireTime()) {
                            it.remove();
                        }
                    } else {
                        // assert false
                        if (value == null) {
                            logger.error("key " + en.getKey() + " is null");
                        } else {
                            logger.error("value of key " + en.getKey() + " is not a CacheValueHolder. type=" + value.getClass());
                        }
                    }
                }
            }finally {
                lock.unlock();
            }
        }

        @Override
        public Object getValue(Object key) {
            Lock lock = readWriteLock.readLock();
            lock.lock();
            try{
                return get(key);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public Map getAllValues(Collection keys) {
            Lock lock = readWriteLock.readLock();
            lock.lock();
            Map values = new HashMap();
            try{
                for (Object key : keys) {
                    Object v = get(key);
                    if (v != null) {
                        values.put(key, v);
                    }
                }
            }finally {
                lock.unlock();
            }
            return values;
        }

        @Override
        public void putValue(Object key, Object value) {
            Lock lock = readWriteLock.writeLock();
            lock.lock();
            try{
                put(key, value);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public void putAllValues(Map map) {
            /*
             * 如果缓存 key 不存在，或者对应的 value 已经失效则放入，否则返回 false
             */
            Lock lock = readWriteLock.writeLock();
            lock.lock();
            try{
                Set<Map.Entry> set = map.entrySet();
                for (Map.Entry en : set) {
                    put(en.getKey(), en.getValue());
                }
            }finally {
                lock.unlock();
            }
        }

        @Override
        public boolean removeValue(Object key) {
            Lock lock = readWriteLock.writeLock();
            lock.lock();
            try{
                return remove(key) != null;
            }finally {
                lock.unlock();
            }
        }

        @Override
        public void removeAllValues(Collection keys) {
            Lock lock = readWriteLock.writeLock();
            lock.lock();
            try{
                for (Object k : keys) {
                    remove(k);
                }
            }finally {
                lock.unlock();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean putIfAbsentValue(Object key, Object value) {
            Lock lock = readWriteLock.writeLock();
            lock.lock();
            try{
                CacheValueHolder h = (CacheValueHolder) get(key);
                if (h == null || parseHolderResult(h).getResultCode() == CacheResultCode.EXPIRED) {
                    put(key, value);
                    return true;
                } else {
                    return false;
                }
            }finally {
                lock.unlock();
            }
        }
    }


}

