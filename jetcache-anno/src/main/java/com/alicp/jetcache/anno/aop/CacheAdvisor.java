/**
 * Created on  13-09-19 20:40
 */
package com.alicp.jetcache.anno.aop;

import com.alicp.jetcache.anno.support.ConfigMap;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractBeanFactoryPointcutAdvisor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author huangli
 */
public class CacheAdvisor extends AbstractBeanFactoryPointcutAdvisor {

    public static final String CACHE_ADVISOR_BEAN_NAME = "jetcache2.internalCacheAdvisor";

    // 注入缓存配置映射
    @Autowired
    private ConfigMap cacheConfigMap;

    // 基础包路径
    private String[] basePackages;

    @Override
    public Pointcut getPointcut() {
        // 切点, 里面判断哪些类需要被拦截, 判断哪些类中的哪些方法会被拦截
        CachePointcut pointcut = new CachePointcut(basePackages); // 创建CachePointcut对象
        pointcut.setCacheConfigMap(cacheConfigMap);  // 设置CachePointcut的缓存配置映射
        return pointcut;
    }

    /**
     * 设置缓存配置映射。
     *
     * @param cacheConfigMap 缓存配置映射
     */
    public void setCacheConfigMap(ConfigMap cacheConfigMap) {
        this.cacheConfigMap = cacheConfigMap;
    }

    public void setBasePackages(String[] basePackages) {
        this.basePackages = basePackages;
    }
}
