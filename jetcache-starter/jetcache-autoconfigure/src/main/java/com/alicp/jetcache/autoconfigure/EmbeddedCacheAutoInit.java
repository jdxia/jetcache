package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.anno.CacheConsts;
import com.alicp.jetcache.embedded.EmbeddedCacheBuilder;

/**
 * Created on 2016/12/2.
 *
 * @author huangli
 */
public abstract class EmbeddedCacheAutoInit extends AbstractCacheAutoInit {

    public EmbeddedCacheAutoInit(String... cacheTypes) {
        super(cacheTypes);
    }

    // 解析本地缓存单有的配置limit
    @Override
    protected void parseGeneralConfig(CacheBuilder builder, ConfigTree ct) {
        super.parseGeneralConfig(builder, ct);
        EmbeddedCacheBuilder ecb = (EmbeddedCacheBuilder) builder;

        // 设置本地缓存每个缓存实例的缓存数量个数限制（默认100）
        ecb.limit(Integer.parseInt(ct.getProperty("limit", String.valueOf(CacheConsts.DEFAULT_LOCAL_LIMIT))));
    }
}
