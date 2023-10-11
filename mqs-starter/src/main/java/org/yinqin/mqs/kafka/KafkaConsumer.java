package org.yinqin.mqs.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.yinqin.mqs.common.Consts;
import org.yinqin.mqs.common.config.MqsProperties.KafkaProperties;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * &#064;description:
 * &#064;author: YinQin
 * &#064;date: 2023-10-10 15:19
 */
public class KafkaConsumer implements MessageConsumer {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final KafkaProperties kafkaProperties;

    private final Map<String, MessageHandler> messageHandlers;

    private final Map<String, MessageHandler> transactionHandlers;

    private final Map<String, MessageHandler> broadcastHandlers;

    private final List<org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]>> consumerList = new ArrayList<>();
    private final List<PollWorker> pollWorkerList = new ArrayList<>();

    private final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

    public KafkaConsumer(KafkaProperties kafkaProperties, Map<String, MessageHandler> messageHandlers, Map<String, MessageHandler> transactionHandlers, Map<String, MessageHandler> broadcastHandlers) {
        this.kafkaProperties = kafkaProperties;
        this.messageHandlers = messageHandlers;
        this.transactionHandlers = transactionHandlers;
        this.broadcastHandlers = broadcastHandlers;
        executor.setCorePoolSize(kafkaProperties.getPollTaskConfig().getCorePoolSize());
        // 设置最大线程数
        executor.setMaxPoolSize(kafkaProperties.getPollTaskConfig().getMaxPoolSize());
        // 设置队列容量
        executor.setQueueCapacity(kafkaProperties.getPollTaskConfig().getQueueCapacity());
        // 设置线程活跃时间（秒）
        executor.setKeepAliveSeconds(kafkaProperties.getPollTaskConfig().getKeepAliveSeconds());
        // 设置线程名称
        executor.setThreadNamePrefix("kafka-poll-thread");
        // 设置拒绝策略
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
    }

    @Override
    public void start() throws Exception {
        if (!messageHandlers.isEmpty()) {
            consumerList.add(createConsumer(Consts.BATCH));
        }
        if (!transactionHandlers.isEmpty()) {
            consumerList.add(createConsumer(Consts.TRAN));
        }
        if (!broadcastHandlers.isEmpty()) {
            consumerList.add(createConsumer(Consts.BROADCAST));
        }
    }

    @Override
    public void destroy() throws Exception {
        consumerList.forEach(org.apache.kafka.clients.consumer.KafkaConsumer::close);
        pollWorkerList.forEach(PollWorker::shutdown);
    }

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> createConsumer(String consumerType) {
        Properties properties = kafkaProperties.getClientConfig();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        if (consumerType.equals(Consts.BATCH)) {
            properties.setProperty("group.id", kafkaProperties.getGroupName());
        }
        if (consumerType.equals(Consts.TRAN)) {
            properties.setProperty("max.poll.records", "1");
            properties.setProperty("group.id", kafkaProperties.getGroupName() + "_TRAN");
        }
        if (consumerType.equals(Consts.BROADCAST)) {
            properties.setProperty("max.poll.records", "1");
            properties.setProperty("group.id", kafkaProperties.getGroupName() + "_BROADCAST_" + properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
        }

        org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        Set<String> topicNames = messageHandlers.keySet();
        kafkaConsumer.subscribe(topicNames);
        PollWorker pollWorker = new PollWorker(consumerType, kafkaConsumer, messageHandlers);
        executor.execute(pollWorker);
        pollWorkerList.add(pollWorker);
        return kafkaConsumer;
    }

}
