package com.alicp.jetcache.redisson;

import com.alicp.jetcache.CacheManager;
import com.alicp.jetcache.CacheResult;
import com.alicp.jetcache.mq.JetCacheMQConsumerStarter;
import com.alicp.jetcache.support.BroadcastManager;
import com.alicp.jetcache.support.CacheMessage;
import com.alicp.jetcache.support.SquashedLogger;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

public class MQBroadcastManager extends BroadcastManager {
    private static final Logger logger = LoggerFactory.getLogger(MQBroadcastManager.class);
    private final RedissonCacheConfig<?, ?> config;
    private final String channel;

    private volatile int subscribeId;

    private final RocketMQClientTemplate mqClientTemplate;
    private final JetCacheMQConsumerStarter mqConsumerStarter;
    private PushConsumer pushConsumer;

    private final ReentrantLock reentrantLock = new ReentrantLock();

    public MQBroadcastManager(final CacheManager cacheManager, final RedissonCacheConfig<?, ?> config) {
        super(cacheManager);
        checkConfig(config);
        this.config = config;
        this.channel = config.getBroadcastChannel();
        this.mqClientTemplate = config.getRocketMQClientTemplate();
        this.mqConsumerStarter = config.getMqConsumerStarter();
    }

    @Override
    public void startSubscribe() {
        reentrantLock.lock();
        if (this.subscribeId == 0 && Objects.nonNull(this.channel) && !this.channel.isEmpty()) {
            this.subscribeId = mqConsumerStarter.hashCode();
            try {
                this.pushConsumer = mqConsumerStarter.startConsumer(config.getMqTopic(), this.channel, messageView -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("jetcache mq receive topic: {}, tag: {}, messageId: {}", messageView.getTopic(), messageView.getTag(), messageView.getMessageId());
                    }

                    ByteBuffer bodyBuffer = messageView.getBody();
                    // 确保ByteBuffer是可读的状态
                    bodyBuffer.rewind();

                    // 创建一个新的byte数组，其长度为ByteBuffer的容量
                    byte[] bodyArray = new byte[bodyBuffer.remaining()];

                    // 将ByteBuffer的内容复制到byte数组中
                    bodyBuffer.get(bodyArray);

                    processNotification(bodyArray, config.getValueDecoder());

                    return ConsumeResult.SUCCESS;
                });


            } catch (Exception e) {
                SquashedLogger.getLogger(logger).error("jetcache subscribe error", e);
            } finally {
                reentrantLock.unlock();
            }
        }
    }


    @Override
    public void close() {
        reentrantLock.lock();
        try {
            final int id;
            if ((id = this.subscribeId) > 0 && Objects.nonNull(this.channel)) {
                try {

                    this.mqClientTemplate.destroy();
                    this.pushConsumer.close();

                } catch (Throwable e) {
                    logger.warn("unsubscribe {} fail", this.channel, e);
                }
                this.subscribeId = 0;
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public CacheResult publish(final CacheMessage cacheMessage) {
        try {
            if (Objects.nonNull(this.channel) && Objects.nonNull(cacheMessage)) {

                final byte[] msg = this.config.getValueEncoder().apply(cacheMessage);

                // rocketmq 发送消息
                String mqDestination = config.getMqTopic() + ":" + this.channel;
                CompletableFuture<SendReceipt> sendReceiptCompletableFuture = this.mqClientTemplate.asyncSendNormalMessage(mqDestination, msg, null);

                if (logger.isDebugEnabled() && Objects.nonNull(cacheMessage.getKeys()) && cacheMessage.getKeys().length != 0) {
                    sendReceiptCompletableFuture.thenAccept(sendReceipt -> {
                        Arrays.stream(cacheMessage.getKeys()).forEach(k -> {
                            logger.debug("jetcache mq async publish message to topic: {}, messageId: {}, msg: {}", mqDestination, sendReceipt.getMessageId(), k);
                        });
                    });
                }

                return CacheResult.SUCCESS_WITHOUT_MSG;
            }
            return CacheResult.FAIL_WITHOUT_MSG;
        } catch (Throwable e) {
            SquashedLogger.getLogger(logger).error("jetcache publish error", e);
            return new CacheResult(e);
        }
    }

}
