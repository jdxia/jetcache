package com.alicp.jetcache.anno.config;

import com.alicp.jetcache.anno.aop.CacheAdvisor;
import com.alicp.jetcache.anno.aop.JetCacheInterceptor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.context.annotation.Role;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;


@Configuration
public class JetCacheProxyConfiguration implements ImportAware, ApplicationContextAware {

    protected AnnotationAttributes enableMethodCache;
    private ApplicationContext applicationContext;

    @Override
    public void setImportMetadata(AnnotationMetadata importMetadata) {
        // 获取 @EnableMethodCache 注解信息
        this.enableMethodCache = AnnotationAttributes.fromMap(
                importMetadata.getAnnotationAttributes(EnableMethodCache.class.getName(), false));
        if (this.enableMethodCache == null) {
            throw new IllegalArgumentException(
                    "@EnableMethodCache is not present on importing class " + importMetadata.getClassName());
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * bean的名称：jetcache2.internalCacheAdvisor
     *
     * 创建一个名为CACHE_ADVISOR_BEAN_NAME的CacheAdvisor Bean，并设置其角色为BeanDefinition.ROLE_INFRASTRUCTURE。
     * 使用JetCacheInterceptor创建CacheAdvisor的缓存拦截器。
     * 设置CacheAdvisor的缓存拦截器为jetCacheInterceptor。
     * 设置CacheAdvisor的基包为enableMethodCache.getStringArray("basePackages")。
     * 设置CacheAdvisor的顺序为enableMethodCache.getNumber("order")。
     * 返回创建的CacheAdvisor Bean。
     */
    @Bean(name = CacheAdvisor.CACHE_ADVISOR_BEAN_NAME)
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public CacheAdvisor jetcacheAdvisor(JetCacheInterceptor jetCacheInterceptor) {
        CacheAdvisor advisor = new CacheAdvisor();
        // 设置缓存拦截器为 JetCacheInterceptor
        advisor.setAdvice(jetCacheInterceptor);
        // 设置需要扫描的包
        advisor.setBasePackages(this.enableMethodCache.getStringArray("basePackages"));
        // 设置优先级，默认 Integer 的最大值，最低优先级
        advisor.setOrder(this.enableMethodCache.<Integer>getNumber("order"));
        return advisor;
    }

    /**
     * 创建一个名为jetCacheInterceptor的JetCacheInterceptor Bean，并设置其角色为BeanDefinition.ROLE_INFRASTRUCTURE。
     * 返回创建的JetCacheInterceptor Bean。
     */
    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public JetCacheInterceptor jetCacheInterceptor() {
        return new JetCacheInterceptor();
    }

}