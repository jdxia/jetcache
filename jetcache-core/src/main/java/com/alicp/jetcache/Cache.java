package com.alicp.jetcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The cache interface, null value is supported.
 *
 * @author huangli
 */
// JSR107 style API, JSR107的javax.cache.Cache接口一致的方法
public interface Cache<K, V> extends Closeable {

    Logger logger = LoggerFactory.getLogger(Cache.class);

    //-----------------------------JSR 107 style API------------------------------------------------

    /**
     * 从缓存中获取一个条目。
     * <p>如果缓存的构造器指定了一个{@link CacheLoader}，并且缓存中没有关联，
     * 它会尝试加载该条目。</p>
     * <p>如果在缓存访问过程中发生错误，方法会返回null而不是抛出异常。</p>
     * @param key 要返回其关联值的键
     * @return 与指定键关联的值。null可能表示：<ul>
     *     <li>条目不存在或已过期</li>
     *     <li>条目的值为null</li>
     *     <li>在缓存访问过程中发生错误（不抛出异常）</li>
     * </ul>
     * @throws CacheInvokeException 仅当加载器抛出异常时
     * @see CacheLoader
     * @see #GET(Object)
     */
    default V get(K key) throws CacheInvokeException {
        CacheGetResult<V> result = GET(key);
        if (result.isSuccess()) {
            return result.getValue();
        } else {
            return null;
        }
    }

    /**
     * 从缓存中获取一组条目，将它们作为与请求的键集相关联的值的Map返回。
     * <p>如果缓存的构造器指定了一个{@link CacheLoader}，并且缓存中没有关联，
     * 它会尝试加载条目。</p>
     * <p>如果在缓存访问过程中发生错误，方法不会抛出异常。</p>
     * @param keys 要返回其关联值的键集合。
     * @return 为给定键找到的条目的映射。在缓存中未找到的键不包含在返回的映射中。
     * @throws CacheInvokeException 仅当加载器抛出异常时
     * @see CacheLoader
     * @see #GET_ALL(Set)
     */
    default Map<K, V> getAll(Set<? extends K> keys) throws CacheInvokeException {
        MultiGetResult<K, V> cacheGetResults = GET_ALL(keys);
        return cacheGetResults.unwrapValues();
    }

    /**
     * 将指定的值与指定的键在缓存中关联起来。
     * <p>如果在缓存访问过程中发生错误，方法不会抛出异常。</p>
     * <p>如果实现支持异步操作，此方法的缓存操作为异步。</p>
     * @param key 与指定值关联的键
     * @param value 要与指定键关联的值
     * @see #PUT(Object, Object)
     */
    default void put(K key, V value) {
        PUT(key, value);
    }

    /**
     * 将指定映射中的所有条目复制到缓存中。
     * <p>如果在缓存访问过程中发生错误，方法不会抛出异常。</p>
     * <p>如果实现支持异步操作，此方法的缓存操作为异步。</p>
     * @param map 要存储在此缓存中的映射。
     * @see #PUT_ALL(Map)
     */
    default void putAll(Map<? extends K, ? extends V> map) {
        PUT_ALL(map);
    }

    /**
     * 原子地将指定的键与给定的值关联，如果它还没有与一个值关联的话。
     * <p>如果在缓存访问过程中发生错误，方法不会抛出异常。</p>
     * <p>{@link MultiLevelCache} 不支持此方法。</p>
     * @param key 要与指定的值关联的键
     * @param value 要与指定的键关联的值
     * @return 如果设置了值，则为true；如果KV关联在缓存中不存在，或在缓存访问过程中发生错误，则为false。
     * @see #PUT_IF_ABSENT(Object, Object, long, TimeUnit)
     */
    default boolean putIfAbsent(K key, V value) {  // 多级缓存MultiLevelCache不支持此方法
        CacheResult result = PUT_IF_ABSENT(key, value, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
        return result.getResultCode() == CacheResultCode.SUCCESS;
    }

    /**
     * Removes the mapping for a key from this cache if it is present.
     * <p>If error occurs during cache access, the method will not throw an exception.</p>
     * @param key key whose mapping is to be removed from the cache
     * @return true if the key is removed successfully, false if the KV association does not exists in the cache,
     *         or error occurs during cache access.
     * @see #REMOVE(Object)
     */
    default boolean remove(K key) {
        return REMOVE(key).isSuccess();
    }

    /**
     * Removes entries for the specified keys.
     * <p>If error occurs during cache access, the method will not throw an exception.</p>
     * <p>if the implementation supports asynchronous operation, the cache operation of this method is asynchronous.</p>
     * @param keys the keys to remove
     * @see #REMOVE_ALL(Set)
     */
    default void removeAll(Set<? extends K> keys) {
        REMOVE_ALL(keys);
    }

    /**
     * Provides a standard way to access the underlying concrete cache entry
     * implementation in order to provide access to further, proprietary features.
     * <p>
     * If the implementation does not support the specified class,
     * the {@link IllegalArgumentException} is thrown.
     *
     * @param clazz the proprietary class or interface of the underlying
     *              concrete cache. It is this type that is returned.
     * @return an instance of the underlying concrete cache
     * @throws IllegalArgumentException if the caching provider doesn't support
     *                                  the specified class.
     */
    <T> T unwrap(Class<T> clazz);

    /**
     * Clean resources created by this cache.
     */
    @Override
    default void close() {
    }

    //--------------------------JetCache API---------------------------------------------

    /**
     * Get the config of this cache.
     * @return the cache config
     */
    CacheConfig<K, V> config();

    /**
     * 尝试使用缓存获取指定键的独占锁，此方法不会阻塞。
     * 用法示例：
     * <pre>
     *   try(AutoReleaseLock lock = cache.tryLock("MyKey",100, TimeUnit.SECONDS)){
     *      if(lock != null){
     *          // 执行某些操作
     *      }
     *   }
     * </pre>
     * <p>{@link MultiLevelCache} 将使用最后一级缓存来支持此操作。</p>
     * @param key      锁键
     * @param expire   锁的过期时间
     * @param timeUnit 锁的过期时间单位
     * @return 如果成功获取锁，则返回一个 AutoReleaseLock 实例（实现了 java.lang.AutoCloseable 接口）。
     *         如果尝试失败（表示另一个线程/进程/服务器已持有锁），或在访问缓存时发生错误，则返回 null。
     * @see #tryLockAndRun(Object, long, TimeUnit, Runnable)
     */
    @SuppressWarnings("unchecked")
    default AutoReleaseLock tryLock(K key, long expire, TimeUnit timeUnit) {
        if (key == null) {
            return null;
        }
        // 生成唯一的UUID作为锁标识
        final String uuid = UUID.randomUUID().toString();
        // 计算锁的过期时间戳
        final long expireTimestamp = System.currentTimeMillis() + timeUnit.toMillis(expire);
        // 获取缓存配置
        final CacheConfig config = config();

        // 定义一个AutoReleaseLock，它包含解锁逻辑
        AutoReleaseLock lock = () -> { // 创建一把会自动释放资源的锁，实现其 close() 方法
            int unlockCount = 0;
            // 尝试解锁次数
            while (unlockCount++ < config.getTryLockUnlockCount()) {
                // 如果锁未过期，则尝试解锁
                if(System.currentTimeMillis() < expireTimestamp) { // 这把锁还没有过期，则删除
                    // 删除对应的 Key 值
                    // 出现的结果：成功，失败，Key 不存在
                    CacheResult unlockResult = REMOVE(key);
                    // 解锁结果处理
                    if (unlockResult.getResultCode() == CacheResultCode.FAIL
                            || unlockResult.getResultCode() == CacheResultCode.PART_SUCCESS) {
                        // 删除对应的 Key 值过程中出现了异常，则重试
                        logger.info("[tryLock] [{} of {}] [{}] unlock failed. Key={}, msg = {}",
                                unlockCount, config.getTryLockUnlockCount(), uuid, key, unlockResult.getMessage());
                        // retry
                        // 重试解锁
                    } else if (unlockResult.isSuccess()) { // 释放成功
                        logger.debug("[tryLock] [{} of {}] [{}] successfully release the lock. Key={}",
                                unlockCount, config.getTryLockUnlockCount(), uuid, key);
                        return;
                    } else {  // 锁已经被释放了
                        logger.warn("[tryLock] [{} of {}] [{}] unexpected unlock result: Key={}, result={}",
                                unlockCount, config.getTryLockUnlockCount(), uuid, key, unlockResult.getResultCode());
                        return;
                    }
                } else {
                    // 锁已过期
                    logger.info("[tryLock] [{} of {}] [{}] lock already expired: Key={}",
                            unlockCount, config.getTryLockUnlockCount(), uuid, key);
                    return;
                }
            }
        };

        int lockCount = 0;
        Cache cache = this;
        // 尝试加锁次数
        while (lockCount++ < config.getTryLockLockCount()) {
            // 尝试添加锁
            // 该方法仅在键不存在时才会添加成功，否则会添加失败。PUT_IF_ABSENT添加锁不成功有两种可能，
            // 即已经存在该键，或者是缓存中没有该键，但是因为特殊原因（如网络原因导致写入Redis失败）导致写入键值失败。
            // 如果缓存中存在该键，则获取锁失败，返回null。否则会继续尝试获取锁信息
            CacheResult lockResult = cache.PUT_IF_ABSENT(key, uuid, expire, timeUnit);
            // 加锁结果处理
            if (lockResult.isSuccess()) {  // 成功获取到锁
                logger.debug("[tryLock] [{} of {}] [{}] successfully get a lock. Key={}",
                        lockCount, config.getTryLockLockCount(), uuid, key);
                return lock;
            } else if (lockResult.getResultCode() == CacheResultCode.FAIL || lockResult.getResultCode() == CacheResultCode.PART_SUCCESS) {
                // 缓存访问失败时的处理逻辑
                logger.info("[tryLock] [{} of {}] [{}] cache access failed during get lock, will inquiry {} times. Key={}, msg={}",
                        lockCount, config.getTryLockLockCount(), uuid,
                        config.getTryLockInquiryCount(), key, lockResult.getMessage());
                int inquiryCount = 0;
                // 尝试查询次数
                while (inquiryCount++ < config.getTryLockInquiryCount()) {
                    // 尝试查询锁状态
                    CacheGetResult inquiryResult = cache.GET(key);
                    // 查询结果处理
                    if (inquiryResult.isSuccess()) {
                        if (uuid.equals(inquiryResult.getValue())) {
                            // 成功获得锁
                            logger.debug("[tryLock] [{} of {}] [{}] successfully get a lock after inquiry. Key={}",
                                    inquiryCount, config.getTryLockInquiryCount(), uuid, key);
                            return lock;
                        } else {
                            // 不是锁的所有者
                            logger.debug("[tryLock] [{} of {}] [{}] not the owner of the lock, return null. Key={}",
                                    inquiryCount, config.getTryLockInquiryCount(), uuid, key);
                            return null;
                        }
                    } else {
                        logger.info("[tryLock] [{} of {}] [{}] inquiry failed. Key={}, msg={}",
                                inquiryCount, config.getTryLockInquiryCount(), uuid, key, inquiryResult.getMessage());
                        // retry inquiry
                        // 重试查询
                    }
                }
            } else {
                // others holds the lock
                // 已存在表示该锁被其他人占有
                logger.debug("[tryLock] [{} of {}] [{}] others holds the lock, return null. Key={}",
                        lockCount, config.getTryLockLockCount(), uuid, key);
                return null;
            }
        }

        // 所有尝试均未成功获得锁
        logger.debug("[tryLock] [{}] return null after {} attempts. Key={}", uuid, config.getTryLockLockCount(), key);
        return null;
    }

    /**
     * Use this cache to try run an action exclusively.
     * <p>{@link MultiLevelCache} will use the last level cache to support this operation.</p>
     * examples:
     * <pre>
     * cache.tryLock("MyKey",100, TimeUnit.SECONDS),() -&gt; {
     *     //do something
     * });
     * </pre>
     * @param key lockKey
     * @param expire lock expire time
     * @param timeUnit lock expire time unit
     * @param action the action need to execute
     * @return true if successfully get the lock and the action is executed
     */
    /**
     * tryLockAndRun方法会非堵塞的尝试获取一把AutoReleaseLock分布式锁（非严格）,获取过程：
     *
     * 尝试往Redis中设置（已存在无法设置）一个键值对，key为缓存key_#RL#，value为UUID，并设置这个键值对的过期时间为60秒（默认）
     * 如果获取到锁后进行加载任务，也就是重新加载方法并更新远程缓存
     * 该锁实现了java.lang.AutoCloseable接口，使用try-with-resource方式，在执行完加载任务后会自动释放资源，也就是调用close方法将获取锁过程中设置的键值对从Redis中删除
     * 在RefreshCache中会调用该方法，因为如果存在远程缓存需要刷新则需要采用分布式锁的方式
     */
    default boolean tryLockAndRun(K key, long expire, TimeUnit timeUnit, Runnable action){
        try (AutoReleaseLock lock = tryLock(key, expire, timeUnit)) { // 尝试获取锁
            if (lock != null) { // 获取到锁则执行下面的任务
                action.run();
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Gets an entry from the cache.
     * <p>if the implementation supports asynchronous operation, the cache access may not completed after this method
     * return. The invoke of getResultCode()/isSuccess()/getMessage()/getValue() on the result will block until cache
     * operation is completed. Call future() method on the result will get a CompletionStage instance for asynchronous
     * programming.</p>
     * @param key the key
     * @return the result
     */
    CacheGetResult<V> GET(K key);

    /**
     * Gets a collection of entries from the Cache.
     * <p>if the implementation supports asynchronous operation, the cache access may not completed after this method
     * return. The invoke of getResultCode()/isSuccess()/getMessage()/getValue() on the result will block until cache
     * operation is completed. Call future() method on the result will get a CompletionStage instance for asynchronous
     * programming.</p>
     * @param keys the key collection
     * @return the result
     */
    MultiGetResult<K, V> GET_ALL(Set<? extends K> keys);

    /**
     * 如果与给定键关联的有值，则返回该值；否则使用加载器加载值并返回，然后更新缓存。
     * @param key 键
     * @param loader 值加载器
     * @return 与键关联的值
     * @see CacheConfig#isCacheNullValue()
     */
    default V computeIfAbsent(K key, Function<K, V> loader) {
        return computeIfAbsent(key, loader, config().isCacheNullValue());
    }

    /**
     * 如果与给定键关联的有值，则返回该值；否则使用加载器加载值并返回，然后根据参数决定是否更新缓存。
     * @param key 键
     * @param loader 值加载器
     * @param cacheNullWhenLoaderReturnNull 当加载器返回null时，是否将null值放入缓存
     * @return 与键关联的值
     */
    V computeIfAbsent(K key, Function<K, V> loader, boolean cacheNullWhenLoaderReturnNull);

    /**
     * 如果与给定键关联的有值，则返回该值；否则使用加载器加载值并返回，然后根据参数决定是否更新缓存，并设置过期时间。
     * @param key 键
     * @param loader 值加载器
     * @param cacheNullWhenLoaderReturnNull 当加载器返回null时，是否将null值放入缓存
     * @param expireAfterWrite 缓存项的TTL（生存时间）
     * @param timeUnit expireAfterWrite的时间单位
     * @return 与键关联的值
     */
    V computeIfAbsent(K key, Function<K, V> loader, boolean cacheNullWhenLoaderReturnNull, long expireAfterWrite, TimeUnit timeUnit);

    /**
     * Associates the specified value with the specified key in the cache.
     * <p>If error occurs during cache access, the method will not throw an exception.</p>
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expireAfterWrite the TTL(time to live) of the KV association
     * @param timeUnit the time unit of expireAfterWrite
     * @see #PUT(Object, Object, long, TimeUnit)
     */
    default void put(K key, V value, long expireAfterWrite, TimeUnit timeUnit) {
        PUT(key, value, expireAfterWrite, timeUnit);
    }

    /**
     * Associates the specified value with the specified key in the cache.
     * <p>if the implementation supports asynchronous operation, the cache access may not completed after this method
     * return. The invoke of getResultCode()/isSuccess()/getMessage() on the result will block until cache
     * operation is completed. Call future() method on the result will get a CompletionStage instance for asynchronous
     * programming.</p>
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the result
     */
    default CacheResult PUT(K key, V value) {
        if (key == null) {
            return CacheResult.FAIL_ILLEGAL_ARGUMENT;
        }
        return PUT(key, value, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Associates the specified value with the specified key in the cache.
     * <p>if the implementation supports asynchronous operation, the cache access may not completed after this method
     * return. The invoke of getResultCode()/isSuccess()/getMessage() on the result will block until cache
     * operation is completed. Call future() method on the result will get a CompletionStage instance for asynchronous
     * programming.</p>
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expireAfterWrite the TTL(time to live) of the KV association
     * @param timeUnit the time unit of expireAfterWrite
     * @return the result
     */
    CacheResult PUT(K key, V value, long expireAfterWrite, TimeUnit timeUnit);

    /**
     * Copies all of the entries from the specified map to the cache.
     * <p>If error occurs during cache access, the method will not throw an exception.</p>
     * @param map mappings to be stored in this cache.
     * @param expireAfterWrite the TTL(time to live) of the KV association
     * @param timeUnit the time unit of expireAfterWrite
     * @see #PUT_ALL(Map, long, TimeUnit)
     */
    default void putAll(Map<? extends K, ? extends V> map, long expireAfterWrite, TimeUnit timeUnit) {
        PUT_ALL(map, expireAfterWrite, timeUnit);
    }

    /**
     * Copies all of the entries from the specified map to the cache.
     * <p>if the implementation supports asynchronous operation, the cache access may not completed after this method
     * return. The invoke of getResultCode()/isSuccess()/getMessage() on the result will block until cache
     * operation is completed. Call future() method on the result will get a CompletionStage instance for asynchronous
     * programming.</p>
     * @param map mappings to be stored in this cache.
     * @return the result
     */
    default CacheResult PUT_ALL(Map<? extends K, ? extends V> map) {
        if (map == null) {
            return CacheResult.FAIL_ILLEGAL_ARGUMENT;
        }
        return PUT_ALL(map, config().getExpireAfterWriteInMillis(), TimeUnit.MILLISECONDS);
    }


    /**
     * 将指定映射中的所有条目复制到缓存中。
     * <p>如果实现支持异步操作，调用此方法后缓存访问可能并未完成。
     * 可以通过调用结果的getResultCode()/isSuccess()/getMessage()方法进行阻塞等待直到缓存操作完成。
     * 调用结果的future()方法将获取用于异步编程的CompletionStage实例。</p>
     * @param map 要存储在缓存中的映射。
     * @param expireAfterWrite KV关联的TTL（生存时间）
     * @param timeUnit expireAfterWrite的时间单位
     * @return 操作结果
     */
    CacheResult PUT_ALL(Map<? extends K, ? extends V> map, long expireAfterWrite, TimeUnit timeUnit);

    /**
     * 如果缓存中存在指定键的映射，则从缓存中移除该映射。
     * <p>如果实现支持异步操作，调用此方法后缓存访问可能并未完成。
     * 可以通过调用结果的getResultCode()/isSuccess()/getMessage()方法进行阻塞等待直到缓存操作完成。
     * 调用结果的future()方法将获取用于异步编程的CompletionStage实例。</p>
     * @param key 要从缓存中移除映射的键
     * @return 操作结果
     */
    CacheResult REMOVE(K key);

    /**
     * 移除指定键的映射。
     * <p>如果实现支持异步操作，调用此方法后缓存访问可能并未完成。
     * 可以通过调用结果的getResultCode()/isSuccess()/getMessage()方法进行阻塞等待直到缓存操作完成。
     * 调用结果的future()方法将获取用于异步编程的CompletionStage实例。</p>
     * @param keys 要移除的键集合
     * @return 操作结果
     */
    CacheResult REMOVE_ALL(Set<? extends K> keys);

    /**
     * 如果指定键尚未与值关联，则将其与给定值关联。
     * <p>如果实现支持异步操作，调用此方法后缓存访问可能并未完成。
     * 可以通过调用结果的getResultCode()/isSuccess()/getMessage()方法进行阻塞等待直到缓存操作完成。
     * 调用结果的future()方法将获取用于异步编程的CompletionStage实例。</p>
     * @param key 与指定值关联的键
     * @param value 要与指定键关联的值
     * @param expireAfterWrite KV关联的TTL（生存时间）
     * @param timeUnit expireAfterWrite的时间单位
     * @return 如果指定键尚未与值关联，则返回SUCCESS；如果指定键已与值关联，则返回EXISTS；如果发生错误，则返回FAIL。
     */
    CacheResult PUT_IF_ABSENT(K key, V value, long expireAfterWrite, TimeUnit timeUnit);

}
