package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.AbstractCacheBuilder;
import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.anno.KeyConvertor;
import com.alicp.jetcache.anno.support.ParserFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created on 2016/11/29.
 *
 * @author huangli
 */
// 主要实现了Spring的InitializingBean接口，在注入Spring容器时，Spring会调用其afterPropertiesSet方法，完成本地缓存类型和远程缓存类型CacheBuilder构造器的初始化
public abstract class AbstractCacheAutoInit implements InitializingBean {

    private static Logger logger = LoggerFactory.getLogger(AbstractCacheAutoInit.class);

    @Autowired
    protected ConfigurableEnvironment environment;

    @Autowired
    protected AutoConfigureBeans autoConfigureBeans;

    private final ReentrantLock reentrantLock = new ReentrantLock();

    protected String[] typeNames;

    private volatile boolean inited = false;

    public AbstractCacheAutoInit(String... cacheTypes) {
        Objects.requireNonNull(cacheTypes,"cacheTypes can't be null");
        Assert.isTrue(cacheTypes.length > 0, "cacheTypes length is 0");
        this.typeNames = cacheTypes;
    }

    /**
     * 在属性设置之后调用的方法
     *
     * 首先会判断是否已经进行初始化了，如果没有就先用锁，然后处理本地和远程缓存的处理器。
     * 实例autoConfigureBeans中的方法getLocalCacheBuilders()和getRemoteCacheBuilders()是用于获取本地和远程的缓存构建者
     */
    @Override
    public void afterPropertiesSet() {
        // 如果还未初始化
        if (!inited) {
            // 上锁
            reentrantLock.lock();
            try{
                // 如果还未初始化
                if (!inited) {
                    // 这里我们有两个指定前缀 'jetcache.local' 'jetcache.remote'
                    // 处理本地缓存构建器
                    process("jetcache.local.", autoConfigureBeans.getLocalCacheBuilders(), true);
                    // 处理远程缓存构建器
                    process("jetcache.remote.", autoConfigureBeans.getRemoteCacheBuilders(), false);
                    // 设置已初始化标志为true
                    inited = true;
                }
            }finally {
                // 解锁
                reentrantLock.unlock();
            }
        }
    }

    /**
     * 处理缓存区域
     *
     * @param prefix 缓存前缀
     * @param cacheBuilders 缓存构建器的映射
     *
     * 这个函数用于处理缓存区域。首先根据给定的前缀和环境创建一个配置树解析器，并获取缓存区域的属性和名称集合。
     * 然后遍历缓存区域名称集合，获取每个区域的类型，并与给定的类型名称进行比较。
     * 如果类型匹配，则初始化该缓存区域的缓存，并将缓存构建器添加到给定的映射中
     */
    private void process(String prefix, Map cacheBuilders, boolean local) {
        // 创建配置树解析器
        ConfigTree resolver = new ConfigTree(environment, prefix);
        // 获取缓存区域的属性集合
        Map<String, Object> m = resolver.getProperties();

        // 获取缓存区域的名称集合
        // 获取本地或者远程的 area ，这里我一般只有默认的 default
        Set<String> cacheAreaNames = resolver.directChildrenKeys();

        // 遍历缓存区域名称集合
        for (String cacheArea : cacheAreaNames) {
            // 获取缓存区域类型
            // 获取本地或者远程存储类型，例如 caffeine，redis.lettuce
            final Object configType = m.get(cacheArea + ".type");
            // 缓存类型是否和当前 CacheAutoInit 的某一个 typeName 匹配（不同的 CacheAutoInit 会设置一个或者多个 typename）
            boolean match = Arrays.stream(typeNames).anyMatch((tn) -> tn.equals(configType));

            // 如果匹配失败，则继续下个循环
            if (!match) {
                continue;
            }
            // 获取本地或者远程的 area 的子配置项
            ConfigTree ct = resolver.subTree(cacheArea + ".");
            logger.info("init cache area {} , type= {}", cacheArea, typeNames[0]);

            // 重点
            // 初始化缓存区域的缓存
            // 根据配置信息构建本地或者远程缓存的 CacheBuilder 构造器
            CacheBuilder c = initCache(ct, local ? "local." + cacheArea : "remote." + cacheArea);
            // 将 CacheBuilder 构造器存放至 AutoConfigureBeans
            cacheBuilders.put(cacheArea, c);
        }
    }

    /**
     * 设置公共的配置到 CacheBuilder 构造器中
     *
     * @param builder 构造器
     * @param ct      配置信息
     */
    protected void parseGeneralConfig(CacheBuilder builder, ConfigTree ct) {
        AbstractCacheBuilder acb = (AbstractCacheBuilder) builder;
        // 设置 Key 的转换函数
        acb.keyConvertor(new ParserFunction(ct.getProperty("keyConvertor", KeyConvertor.FASTJSON2)));

        // 设置超时时间
        String expireAfterWriteInMillis = ct.getProperty("expireAfterWriteInMillis");
        if (expireAfterWriteInMillis == null) {
            // compatible with 2.1
            // compatible with 2.1 兼容老版本
            expireAfterWriteInMillis = ct.getProperty("defaultExpireInMillis");
        }
        if (expireAfterWriteInMillis != null) {
            acb.setExpireAfterWriteInMillis(Long.parseLong(expireAfterWriteInMillis));
        }

        // 多长时间没有访问就让缓存失效，0表示不使用该功能（注意：只支持本地缓存）
        String expireAfterAccessInMillis = ct.getProperty("expireAfterAccessInMillis");
        if (expireAfterAccessInMillis != null) {
            acb.setExpireAfterAccessInMillis(Long.parseLong(expireAfterAccessInMillis));
        }

    }

    /**
     * 初始化 CacheBuilder 构造器交由子类去实现
     *
     * @param ct                  配置信息
     * @param cacheAreaWithPrefix 配置前缀
     * @return CacheBuilder 构造器
     */
    protected abstract CacheBuilder initCache(ConfigTree ct, String cacheAreaWithPrefix);
}
