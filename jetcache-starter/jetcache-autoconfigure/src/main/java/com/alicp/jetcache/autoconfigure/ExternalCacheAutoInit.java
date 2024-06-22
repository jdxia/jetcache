package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.anno.CacheConsts;
import com.alicp.jetcache.anno.support.ParserFunction;
import com.alicp.jetcache.external.ExternalCacheBuilder;

/**
 * Created on 2016/11/29.
 *
 * @author huangli
 */
/**
 * 类ExternalCacheAutoInit作为Redis缓存自动化初始类的父类
 *
 * 主要是覆盖父类的parseGeneralConfig，解析远程缓存单有的配置keyPrefix、valueEncoder和valueDecoder
 */
public abstract class ExternalCacheAutoInit extends AbstractCacheAutoInit {
    public ExternalCacheAutoInit(String... cacheTypes) {
        super(cacheTypes);
    }

    /**
     * 设置远程缓存 CacheBuilder 构造器的相关配置
     *
     * @param builder 构造器
     * @param ct      配置信息
     */
    @Override
    protected void parseGeneralConfig(CacheBuilder builder, ConfigTree ct) {
        super.parseGeneralConfig(builder, ct);
        ExternalCacheBuilder ecb = (ExternalCacheBuilder) builder;
        // 设置远程缓存 key 的前缀
        ecb.setKeyPrefix(ct.getProperty("keyPrefix"));
        /*
         * 根据配置创建缓存数据的编码函数和解码函数
         *
         * parseBroadcastChannel 里面有解析发布订阅通道的
         */
        ecb.setBroadcastChannel(parseBroadcastChannel(ct));
        ecb.setValueEncoder(new ParserFunction(ct.getProperty("valueEncoder", CacheConsts.DEFAULT_SERIAL_POLICY)));
        ecb.setValueDecoder(new ParserFunction(ct.getProperty("valueDecoder", CacheConsts.DEFAULT_SERIAL_POLICY)));
    }

    /**
     * 解析广播通道
     *
     * @param ct 配置信息
     * @return 广播通道
     */
    protected String parseBroadcastChannel(ConfigTree ct) {
        String broadcastChannel = ct.getProperty("broadcastChannel");
        if (broadcastChannel != null && !"".equals(broadcastChannel.trim())) {
            return broadcastChannel.trim();
        } else {
            return null;
        }
    }
}
