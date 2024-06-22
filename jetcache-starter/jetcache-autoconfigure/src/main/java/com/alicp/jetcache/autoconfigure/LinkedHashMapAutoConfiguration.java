package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.embedded.LinkedHashMapCacheBuilder;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Created on 2016/12/2.
 *
 * @author huangli
 */
@Component
// 条件是LinkedHashMapCondition，继承了JetCacheCondition
// 如果spring boot项目的application.yml文件中jetcache.local.${areaName} .type或jetcache.remote.${areaName}.type为linkedhashmap时，就会构建LinkedHashMapAutoConfiguration对应的bean
@Conditional(LinkedHashMapAutoConfiguration.LinkedHashMapCondition.class)
public class LinkedHashMapAutoConfiguration extends EmbeddedCacheAutoInit {
    public LinkedHashMapAutoConfiguration() {
        super("linkedhashmap");
    }

    /**
     * 初始化缓存
     * @param ct            配置树
     * @param cacheAreaWithPrefix    缓存区域名称（包括前缀）
     * @return              初始化后的缓存
     */
    @Override
    protected CacheBuilder initCache(ConfigTree ct, String cacheAreaWithPrefix) {
        // 创建一个 LinkedHashMapCacheBuilder 构造器
        LinkedHashMapCacheBuilder builder = LinkedHashMapCacheBuilder.createLinkedHashMapCacheBuilder();
        // 解析相关配置至 LinkedHashMapCacheBuilder 的 CacheConfig 中
        parseGeneralConfig(builder, ct);
        return builder;
    }

    /**
     * LinkedHashMap条件类
     */
    public static class LinkedHashMapCondition extends JetCacheCondition {
        // 配置了缓存类型为 linkedhashmap 当前类才会被注入 Spring 容器
        public LinkedHashMapCondition() {
            super("linkedhashmap");
        }
    }
}
