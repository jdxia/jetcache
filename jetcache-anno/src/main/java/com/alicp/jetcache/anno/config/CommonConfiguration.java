/**
 * Created on 2019/6/23.
 */
package com.alicp.jetcache.anno.config;

import com.alicp.jetcache.anno.support.ConfigMap;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

/**
 * @author huangli
 */
@Configuration
public class CommonConfiguration {

    //保存方法与缓存注解配置信息的映射关系
    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public ConfigMap jetcacheConfigMap() {
        return new ConfigMap();
    }
}
