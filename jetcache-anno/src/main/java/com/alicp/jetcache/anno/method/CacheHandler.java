/**
 * Created on  13-09-09 15:59
 */
package com.alicp.jetcache.anno.method;

import com.alicp.jetcache.AbstractCache;
import com.alicp.jetcache.Cache;
import com.alicp.jetcache.CacheInvokeException;
import com.alicp.jetcache.CacheLoader;
import com.alicp.jetcache.ProxyCache;
import com.alicp.jetcache.anno.support.CacheContext;
import com.alicp.jetcache.anno.support.CacheInvalidateAnnoConfig;
import com.alicp.jetcache.anno.support.CacheUpdateAnnoConfig;
import com.alicp.jetcache.anno.support.CachedAnnoConfig;
import com.alicp.jetcache.anno.support.ConfigMap;
import com.alicp.jetcache.event.CacheLoadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class CacheHandler implements InvocationHandler {
    private static Logger logger = LoggerFactory.getLogger(CacheHandler.class);

    private Object src;
    private Supplier<CacheInvokeContext> contextSupplier;
    private String[] hiddenPackages;
    private ConfigMap configMap;

    private static class CacheContextSupport extends CacheContext {

        public CacheContextSupport() {
            super(null, null, null);
        }

        static void _enable() {
            enable();
        }

        static void _disable() {
            disable();
        }

        static boolean _isEnabled() {
            return isEnabled();
        }
    }

    public CacheHandler(Object src, ConfigMap configMap, Supplier<CacheInvokeContext> contextSupplier, String[] hiddenPackages) {
        this.src = src;
        this.configMap = configMap;
        this.contextSupplier = contextSupplier;
        this.hiddenPackages = hiddenPackages;
    }

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
        CacheInvokeContext context = null;

        String sig = ClassUtil.getMethodSig(method);
        CacheInvokeConfig cac = configMap.getByMethodInfo(sig);
        if (cac != null) {
            context = contextSupplier.get();
            context.setCacheInvokeConfig(cac);
        }
        if (context == null) {
            return method.invoke(src, args);
        } else {
            context.setInvoker(() -> method.invoke(src, args));
            context.setHiddenPackages(hiddenPackages);
            context.setArgs(args);
            context.setMethod(method);
            return invoke(context);
        }
    }

    public static Object invoke(CacheInvokeContext context) throws Throwable {
        if (context.getCacheInvokeConfig().isEnableCacheContext()) {
            try {
                CacheContextSupport._enable();
                return doInvoke(context);
            } finally {
                CacheContextSupport._disable();
            }
        } else {
            return doInvoke(context); // 核心
        }
    }

    private static Object doInvoke(CacheInvokeContext context) throws Throwable {
        // 获取缓存实例配置
        CacheInvokeConfig cic = context.getCacheInvokeConfig();
        // 获取注解配置信息
        CachedAnnoConfig cachedConfig = cic.getCachedAnnoConfig();
        if (cachedConfig != null && (cachedConfig.isEnabled() || CacheContextSupport._isEnabled())) {
            // 使用了@Cached注解, 经过缓存中获取结果
            return invokeWithCached(context);
        } else if (cic.getInvalidateAnnoConfigs() != null || cic.getUpdateAnnoConfig() != null) {
            // 使用了@CacheUpdate、@CacheInvalidate注解
            // 根据结果删除或者更新缓存
            return invokeWithInvalidateOrUpdate(context);
        } else {
            // 没使用上述三个注解
            // 执行该方法
            return invokeOrigin(context);
        }
    }

    private static Object invokeWithInvalidateOrUpdate(CacheInvokeContext context) throws Throwable {
        Object originResult = invokeOrigin(context);
        context.setResult(originResult);
        CacheInvokeConfig cic = context.getCacheInvokeConfig();

        // 注意下面是@CacheInvalidate的多个配置
        if (cic.getInvalidateAnnoConfigs() != null) {
            // 核心
            doInvalidate(context, cic.getInvalidateAnnoConfigs());
        }
        CacheUpdateAnnoConfig updateAnnoConfig = cic.getUpdateAnnoConfig();
        if (updateAnnoConfig != null) {
            doUpdate(context, updateAnnoConfig);
        }

        return originResult;
    }

    private static Iterable toIterable(Object obj) {
        if (obj.getClass().isArray()) {
            if (obj instanceof Object[]) {
                return Arrays.asList((Object[]) obj);
            } else {
                List list = new ArrayList();
                int len = Array.getLength(obj);
                for (int i = 0; i < len; i++) {
                    list.add(Array.get(obj, i));
                }
                return list;
            }
        } else if (obj instanceof Iterable) {
            return (Iterable) obj;
        } else {
            return null;
        }
    }

    // 清除指定缓存
    private static void doInvalidate(CacheInvokeContext context, List<CacheInvalidateAnnoConfig> annoConfig) {
        // 配置几个CacheInvalidate注解就会失效几个缓存，但是Update操作却不支持
        for (CacheInvalidateAnnoConfig config : annoConfig) {
            doInvalidate(context, config);
        }
    }

    // 清除单个缓存
    private static void doInvalidate(CacheInvokeContext context, CacheInvalidateAnnoConfig annoConfig) {
        Cache cache = context.getCacheFunction().apply(context, annoConfig);  // 获取指定缓存
        if (cache == null) {
            return;
        }
        boolean condition = ExpressionUtil.evalCondition(context, annoConfig);  // 判断是否满足条件
        if (!condition) {
            return;
        }
        Object key = ExpressionUtil.evalKey(context, annoConfig);  // 获取缓存的
        if (key == null) {
            return;
        }
        if (annoConfig.isMulti()) {  // 判断是否为多个键
            Iterable it = toIterable(key);  // 将键转为迭代器
            if (it == null) {
                logger.error("jetcache @CacheInvalidate key is not instance of Iterable or array: " + annoConfig.getDefineMethod());
                return;
            }
            Set keys = new HashSet();
            it.forEach(k -> keys.add(k)); // 将键添加到集合中
            cache.removeAll(keys);  // 清除所有指定键的缓存
        } else {
            cache.remove(key);  // 清除指定键的缓存
        }
    }

    /**
     * 这个函数是一个用于更新缓存的方法。首先根据传入的参数获取缓存对象，如果缓存为空，则直接返回。
     * 然后通过判断条件来确定是否执行更新操作。接着通过调用ExpressionUtil的evalKey方法和evalValue方法来获取键和值。如果键或值获取失败，则直接返回。
     * 如果updateAnnoConfig.isMulti()为true，则表示是批量更新操作。这时需要将值转换为可迭代对象，并判断是否为空。
     * 接着将键和值分别转换为列表，并进行大小判断。如果不相等，则打印错误日志并返回。
     * 如果相等，则构建一个键值对映射，然后将该映射批量更新到缓存中。如果updateAnnoConfig.isMulti()为false，则表示是单个更新操作，直接将键值对更新到缓存中
     */
    private static void doUpdate(CacheInvokeContext context, CacheUpdateAnnoConfig updateAnnoConfig) {
        // 获取缓存
        Cache cache = context.getCacheFunction().apply(context, updateAnnoConfig);
        // 若缓存为空，直接返回
        if (cache == null) {
            return;
        }
        // 判断是否满足更新条件
        boolean condition = ExpressionUtil.evalCondition(context, updateAnnoConfig);
        if (!condition) {
            return;
        }
        // 获取键值
        Object key = ExpressionUtil.evalKey(context, updateAnnoConfig);
        Object value = ExpressionUtil.evalValue(context, updateAnnoConfig);
        // 若键值为空或者获取键值失败，直接返回
        if (key == null || value == ExpressionUtil.EVAL_FAILED) {
            return;
        }
        // 判断是否为批量更新
        if (updateAnnoConfig.isMulti()) {
            // 若值为空，直接返回
            if (value == null) {
                return;
            }
            // 将键值转换为可迭代对象
            Iterable keyIt = toIterable(key);
            Iterable valueIt = toIterable(value);
            // 若键为null，打印错误日志并返回
            if (keyIt == null) {
                logger.error("jetcache @CacheUpdate key is not instance of Iterable or array: " + updateAnnoConfig.getDefineMethod());
                return;
            }
            // 若值为null，打印错误日志并返回
            if (valueIt == null) {
                logger.error("jetcache @CacheUpdate value is not instance of Iterable or array: " + updateAnnoConfig.getDefineMethod());
                return;
            }

            // 转换为列表
            List keyList = new ArrayList();
            List valueList = new ArrayList();
            keyIt.forEach(o -> keyList.add(o));
            valueIt.forEach(o -> valueList.add(o));

            // 若键列表与值列表大小不一致，打印错误日志并返回
            if (keyList.size() != valueList.size()) {
                logger.error("jetcache @CacheUpdate key size not equals with value size: " + updateAnnoConfig.getDefineMethod());
                return;
            } else {
                // 构建键值对映射
                Map m = new HashMap();
                for (int i = 0; i < valueList.size(); i++) {
                    m.put(keyList.get(i), valueList.get(i));
                }
                // 批量更新缓存
                cache.putAll(m);
            }
        } else {
            // 批量更新缓存
            cache.put(key, value);
        }
    }

    /**
     * 获取缓存注解信息
     * 根据本地调用的上下文CacheInvokeContext获取缓存实例对象（调用其cacheFunction函数）
     * 如果缓存实例不存在则直接调用invokeOrigin方法，执行被拦截的对象的调用器
     * 根据本次调用的上下文CacheInvokeContext生成缓存key，根据配置的缓存key的SpEL表达式生成，如果没有配置则返回入参对象，如果没有对象则返回"_ $JETCACHE_NULL_KEY$_"
     * 根据配置condition表达式判断是否需要走缓存
     * 创建一个CacheLoader对象，用于执行被拦截的对象的调用器，也就是加载原有方法
     * 调用缓存实例的computeIfAbsent(key, loader)方法获取结果
     */
    private static Object invokeWithCached(CacheInvokeContext context)
            throws Throwable {
        // 获取本地调用的上下文, 获取@Cached注解配置参数
        CacheInvokeConfig cic = context.getCacheInvokeConfig();
        // 获取注解配置信息
        CachedAnnoConfig cac = cic.getCachedAnnoConfig();

        /**
         * 获取缓存实例对象（不存在则会创建并设置到 cac 中）
         * 可在 JetCacheInterceptor 创建本次调用的上下文时，调用
         * {@link com.alicp.jetcache.anno.support.CacheContext#createCacheInvokeContext(com.alicp.jetcache.anno.support.ConfigMap)}
         * 方法中查看详情
         */
        Cache cache = context.getCacheFunction().apply(context, cac);
        if (cache == null) {  // 判断缓存是否为空
            logger.error("no cache with name: " + context.getMethod());
            // 无缓存实例对象，执行原有方法
            return invokeOrigin(context);
        }

        // Spel表达式解析key
        // 生成缓存 Key 对象（注解中没有配置的话就是入参，没有入参则为 "_$JETCACHE_NULL_KEY$_" ）
        Object key = ExpressionUtil.evalKey(context, cic.getCachedAnnoConfig());
        if (key == null) {
            // 生成缓存 Key 失败则执行原方法，并记录 CacheLoadEvent 事件
            return loadAndCount(context, cache, key);
        }

        /*
         * 根据配置的 condition 来决定是否走缓存
         * 缓存注解中没有配置 condition 表示所有请求都走缓存
         * 配置了 condition 表示满足条件的才走缓存
         */
        if (!ExpressionUtil.evalCondition(context, cic.getCachedAnnoConfig())) {
            // 不满足 condition 则直接执行原方法，并记录 CacheLoadEvent 事件
            return loadAndCount(context, cache, key);
        }

        try {
            // 创建一个执行原有方法的函数
            // 这里创建了一个CacheLoader的类实例，该类用于当缓存不存在对应key数据时调用原始方法（读数据库或者调用api请求数据）获取数据
            CacheLoader loader = new CacheLoader() {
                @Override
                public Object load(Object k) throws Throwable {
                    // 原始方法调用
                    Object result = invokeOrigin(context);
                    context.setResult(result);  // 设置调用上下文的结果
                    return result;
                }

                // 判断是否禁止缓存更新
                @Override
                public boolean vetoCacheUpdate() {
                    // 本次执行原方法后是否需要更新缓存
                    // 判断@Cached后置condition条件是否有效，比如如果调用api请求失败，这种情况是无需缓存数据的
                    return !ExpressionUtil.evalPostCondition(context, cic.getCachedAnnoConfig());  // 根据表达式判断是否禁止缓存更新
                }
            };
            // 获取结果
            Object result = cache.computeIfAbsent(key, loader);  // 根据key计算缓存结果
            return result;  // 返回缓存结果
        } catch (CacheInvokeException e) {  // 捕获缓存调用异常
            throw e.getCause();  // 抛出异常原因
        }
    }
    // 主要是执行invokeOrigin，然后发布CacheLoadEvent事件
    private static Object loadAndCount(CacheInvokeContext context, Cache cache, Object key) throws Throwable {
        long t = System.currentTimeMillis();
        Object v = null;
        boolean success = false;
        try {
            // 调用原有方法
            v = invokeOrigin(context);
            success = true;
        } finally {
            t = System.currentTimeMillis() - t;
            // 发送 CacheLoadEvent 事件
            CacheLoadEvent event = new CacheLoadEvent(cache, t, key, v, success);
            while (cache instanceof ProxyCache) {
                cache = ((ProxyCache) cache).getTargetCache();
            }
            if (cache instanceof AbstractCache) {
                ((AbstractCache) cache).notify(event);
            }
        }
        return v;
    }

    private static Object invokeOrigin(CacheInvokeContext context) throws Throwable {
        // 执行被拦截的方法
        return context.getInvoker().invoke();
    }

}
