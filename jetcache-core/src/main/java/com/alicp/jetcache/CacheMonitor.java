package com.alicp.jetcache;

import com.alicp.jetcache.event.CacheEvent;
import com.alicp.jetcache.template.CacheMonitorInstaller;
import com.alicp.jetcache.template.QuickConfig;

/**
 * Created on 2016/10/25.
 *
 * @author huangli
 *
 * {@link com.alicp.jetcache.anno.support.ConfigProvider#doInit()} 到构建cache时执行安装器 {@link SimpleCacheManager#create(QuickConfig)}
 *
 * {@link CacheMonitorInstaller}
 * Monitor 的创建与安装通过“安装器”机制集中完成：在构建 Cache 时，CacheMonitorInstaller 会被调用，把具体的 Monitor 加到 cache.config().getMonitors() 列表里。
 * CacheConfig.setMonitors方法：每个 Cache 都有一个 monitors 列表，安装到这里的 Monitor 会在事件发生后被回调
 * {@link CacheConfig}
 */
@FunctionalInterface
public interface CacheMonitor {

    /**
     * 用于消费CacheEvent，它有两个实现类，分别是DefaultCacheMonitor、CacheNotifyMonitor
     */
    void afterOperation(CacheEvent event);

}
