package com.alicp.jetcache.autoconfigure;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 2016/11/28.
 *
 * @author huangli
 */
public abstract class JetCacheCondition extends SpringBootCondition {

    /**
     * 缓存类型数组
     */
    private String[] cacheTypes;

    /**
     * 构造方法
     * @param cacheTypes 缓存类型数组
     */
    protected JetCacheCondition(String... cacheTypes) {
        Objects.requireNonNull(cacheTypes, "cacheTypes can't be null");
        Assert.isTrue(cacheTypes.length > 0, "cacheTypes length is 0");
        this.cacheTypes = cacheTypes;
    }

    /**
     * 判断条件是否匹配
     * @param conditionContext 条件上下文
     * @param annotatedTypeMetadata 注解类型元数据
     * @return 匹配结果
     */
    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        ConfigTree ct = new ConfigTree((ConfigurableEnvironment) conditionContext.getEnvironment(), "jetcache.");
        if (match(ct, "local.") || match(ct, "remote.")) {
            return ConditionOutcome.match();
        } else {
            return ConditionOutcome.noMatch("no match for " + cacheTypes[0]);
        }
    }

    /**
     * 判断是否匹配指定前缀
     * @param ct 配置树
     * @param prefix 前缀
     * @return 是否匹配
     */
    private boolean match(ConfigTree ct, String prefix) {
        Map<String, Object> m = ct.subTree(prefix).getProperties();
        Set<String> cacheAreaNames = m.keySet().stream().map((s) -> s.substring(0, s.indexOf('.'))).collect(Collectors.toSet());
        final List<String> cacheTypesList = Arrays.asList(cacheTypes);
        return cacheAreaNames.stream().anyMatch((s) -> cacheTypesList.contains(m.get(s + ".type")));
    }
}
