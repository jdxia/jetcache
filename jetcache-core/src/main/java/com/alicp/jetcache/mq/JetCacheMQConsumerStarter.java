package com.alicp.jetcache.mq;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

@Component
public class JetCacheMQConsumerStarter {
    @Value("${rocketmq.push-consumer.endpoints}")
    private String endpoint;

//    @Value("${rocketmq.username}")
//    private String username;
//    @Value("${rocketmq.password}")
//    private String password;

    @Value("${spring.application.name:default}")
    private String appName;

    @Value("${jetcache.consumer.thread:8}")
    private Integer jetCacheConsumerThread;

    private static final Logger log = LoggerFactory.getLogger(JetCacheMQConsumerStarter.class);

    public PushConsumer startConsumer(String topicName, String filterExpression, MessageListener messageListenerCallBack) throws ClientException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoint)
//                .setCredentialProvider(new StaticSessionCredentialsProvider(username, password))
                .build();

        FilterExpression expression = new FilterExpression(filterExpression, FilterExpressionType.TAG);

        // jvm启动时间
        long startTime = ManagementFactory.getRuntimeMXBean().getStartTime();
        // 定义时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSS");

        // 将JVM启动时间戳转换为Instant对象，然后根据默认时区转换为ZonedDateTime
        Instant instant = Instant.ofEpochMilli(startTime);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());

        // 格式化时间
        String formattedStartTime = zonedDateTime.format(formatter);

        String consumerGroupName = "jetcache-" + appName + "-" + formattedStartTime;

        log.info("jetcache mq consumer start, consumerGroupName: {}, topicName: {}, tag: {}", consumerGroupName, topicName, filterExpression);

        return provider.newPushConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(consumerGroupName)
                .setSubscriptionExpressions(Collections.singletonMap(topicName, expression))
                .setConsumptionThreadCount(jetCacheConsumerThread)
                .setMessageListener(messageListenerCallBack)
                .build();


    }
}
