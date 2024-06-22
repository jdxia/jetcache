package com.alicp.jetcache.autoconfigure;

import com.alicp.jetcache.CacheBuilder;
import com.alicp.jetcache.CacheConfigException;
import com.alicp.jetcache.anno.CacheConsts;
import com.alicp.jetcache.external.ExternalCacheBuilder;
import com.alicp.jetcache.redis.lettuce.JetCacheCodec;
import com.alicp.jetcache.redis.lettuce.LettuceConnectionManager;
import com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 2017/5/10.
 *
 * @author huangli
 */
@Configuration
@Conditional(RedisLettuceAutoConfiguration.RedisLettuceCondition.class)
public class RedisLettuceAutoConfiguration {
    public static final String AUTO_INIT_BEAN_NAME = "redisLettuceAutoInit";

    /**
     * 注入 spring 容器的条件
     */
    public static class RedisLettuceCondition extends JetCacheCondition {
        // 配置了缓存类型为 redis.lettuce 当前类才会被注入 Spring 容器
        public RedisLettuceCondition() {
            super("redis.lettuce");
        }
    }

    @Bean(name = {AUTO_INIT_BEAN_NAME})
    public RedisLettuceAutoInit redisLettuceAutoInit() {
        return new RedisLettuceAutoInit();
    }

    /**
     * 解析spring boot项目的application.yml文件中jetcache.remote .${areaName}中的Redis配置，创建Redis连接，如果配置了集群模式，则需要创建集群模式的Redis连接；
     * 如果配置的是单节点模式，则会创建单节点的Redis连接，创建用于创建Lettuce类型的内存缓存的创建者RedisLettuceCacheBuilder，并调用parseGeneralConfig方法解析JetCache的配置。
     * 最后将客户端和连接对象存入自动配置的自定义容器中，并返回一个包含相关配置的ExternalCacheBuilder对象
     */
    public static class RedisLettuceAutoInit extends ExternalCacheAutoInit {

        public RedisLettuceAutoInit() {
            // 设置缓存类型
            super("redis.lettuce");
        }

        /**
         * 初始化 RedisLettuceCacheBuilder 构造器
         *
         * @param ct                  配置信息
         * @param cacheAreaWithPrefix 配置前缀
         * @return 构造器
         */
        @Override
        protected CacheBuilder initCache(ConfigTree ct, String cacheAreaWithPrefix) {
            // 获取Lettuce缓存的URI配置
            Map<String, Object> map = ct.subTree("uri"/*there is no dot*/).getProperties();
            // 数据节点偏好设置
            String readFromStr = ct.getProperty("readFrom");
            // 集群模式
            String mode = ct.getProperty("mode");
            // 异步获取结果的超时时间，默认1s
            long asyncResultTimeoutInMillis = ct.getProperty("asyncResultTimeoutInMillis", CacheConsts.ASYNC_RESULT_TIMEOUT.toMillis());
            // 判断是否启用广播通道
            boolean enablePubSub = parseBroadcastChannel(ct) != null;
            ReadFrom readFrom = null;
            // 根据读取模式字符串获取ReadFrom枚举类型
            if (readFromStr != null) {
                /*
                 * MASTER：只从Master节点中读取。
                 * MASTER_PREFERRED：优先从Master节点中读取。
                 * SLAVE_PREFERRED：优先从Slave节点中读取。
                 * SLAVE：只从Slave节点中读取。
                 * NEAREST：使用最近一次连接的Redis实例读取。
                 */
                readFrom = ReadFrom.valueOf(readFromStr.trim());
            }

            AbstractRedisClient client;
            StatefulConnection<byte[], byte[]> connection;
            StatefulRedisPubSubConnection<byte[], byte[]> pubSubConnection = null;
            // 判断是否配置了URI
            if (map == null || map.size() == 0) {
                // 如果未配置URI，则抛出异常
                throw new CacheConfigException("lettuce uri is required");
            } else {
                // 创建对应的 RedisURI
                // 将配置的URI转换为RedisURI对象列表
                List<RedisURI> uriList = map.values().stream().map((k) -> RedisURI.create(URI.create(k.toString())))
                        .collect(Collectors.toList());

                if ("Cluster".equalsIgnoreCase(mode)) {
                    // 配置为集群模式
                    client = RedisClusterClient.create(uriList);
                    // 链接集群节点并获取连接对象
                    connection = clusterConnection(ct, readFrom, (RedisClusterClient) client, false);
                    if (enablePubSub) {
                        // 如果启用了广播通道，则获取与集群节点的连接对象
                        pubSubConnection = (StatefulRedisPubSubConnection) clusterConnection(ct, readFrom, (RedisClusterClient) client, true);
                    }
                } else {
                    // 配置为单节点模式
                    client = RedisClient.create();
                    ((RedisClient) client).setOptions(ClientOptions.builder().
                            disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build());
                    // 链接单节点并获取连接对象
                    StatefulRedisMasterReplicaConnection c = MasterReplica.connect(
                            (RedisClient) client, new JetCacheCodec(), uriList);
                    if (readFrom != null) {
                        // 如果指定了读取模式，则设置连接对象的读取模式
                        c.setReadFrom(readFrom);
                    }
                    connection = c;
                    if (enablePubSub) {
                        // 如果启用了广播通道，则获取与单节点的连接对象
                        pubSubConnection = ((RedisClient) client).connectPubSub(new JetCacheCodec(), uriList.get(0));
                    }
                }
            }

            // 创建一个 RedisLettuceCacheBuilder 构造器
            // 创建外部缓存构建器对象
            ExternalCacheBuilder externalCacheBuilder = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                    .connection(connection)
                    .pubSubConnection(pubSubConnection)
                    .redisClient(client)
                    .asyncResultTimeoutInMillis(asyncResultTimeoutInMillis);

            // 解析通用配置
            // 解析相关配置至 RedisLettuceCacheBuilder 的 CacheConfig 中
            parseGeneralConfig(externalCacheBuilder, ct);

            // eg: "remote.default.client"
            // 将Redis客户端对象存入自动配置的自定义容器中
            autoConfigureBeans.getCustomContainer().put(cacheAreaWithPrefix + ".client", client);
            // 开始将 Redis 客户端和安全连接保存至 LettuceConnectionManager 管理器中
            LettuceConnectionManager m = LettuceConnectionManager.defaultManager();
            // 初始化 Lettuce 连接 Redis
            m.init(client, connection);
            // 初始化 Redis 连接的相关信息保存至 LettuceObjects 中，并将相关信息保存至 AutoConfigureBeans.customContainer
            autoConfigureBeans.getCustomContainer().put(cacheAreaWithPrefix + ".connection", m.connection(client));
            // 将Lettuce连接管理器中的命令对象存入自动配置的自定义容器中
            autoConfigureBeans.getCustomContainer().put(cacheAreaWithPrefix + ".commands", m.commands(client));
            // 将Lettuce连接管理器中的异步命令对象存入自动配置的自定义容器中
            autoConfigureBeans.getCustomContainer().put(cacheAreaWithPrefix + ".asyncCommands", m.asyncCommands(client));
            // 将Lettuce连接管理器中的反应式命令对象存入自动配置的自定义容器中
            autoConfigureBeans.getCustomContainer().put(cacheAreaWithPrefix + ".reactiveCommands", m.reactiveCommands(client));
            // 返回外部缓存构建器对象
            return externalCacheBuilder;
        }

        /**
         * 创建一个与Redis集群连接的状态保持连接对象。
         * @param ct 配置树对象，用于获取配置信息
         * @param readFrom 读取来源，用于设置连接的读取来源
         * @param client Redis集群客户端对象
         * @param pubsub 是否创建一个订阅连接
         * @return 状态保持连接对象
         */
        private StatefulConnection<byte[], byte[]> clusterConnection(ConfigTree ct, ReadFrom readFrom, RedisClusterClient client, boolean pubsub) {
            int enablePeriodicRefresh = ct.getProperty("enablePeriodicRefresh", 60);
            boolean enableAllAdaptiveRefreshTriggers = ct.getProperty("enableAllAdaptiveRefreshTriggers", true);
            ClusterTopologyRefreshOptions.Builder topologyOptionBuilder = ClusterTopologyRefreshOptions.builder();
            if (enablePeriodicRefresh > 0) {
                topologyOptionBuilder.enablePeriodicRefresh(Duration.ofSeconds(enablePeriodicRefresh));
            }
            if (enableAllAdaptiveRefreshTriggers) {
                topologyOptionBuilder.enableAllAdaptiveRefreshTriggers();
            }

            ClusterClientOptions options = ClusterClientOptions.builder()
                    .topologyRefreshOptions(topologyOptionBuilder.build())
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .build();
            client.setOptions(options);
            if (pubsub) {
                return client.connectPubSub(new JetCacheCodec());
            } else {
                StatefulRedisClusterConnection<byte[], byte[]> c = client.connect(new JetCacheCodec());
                if (readFrom != null) {
                    c.setReadFrom(readFrom);
                }
                return c;
            }
        }
    }
}
