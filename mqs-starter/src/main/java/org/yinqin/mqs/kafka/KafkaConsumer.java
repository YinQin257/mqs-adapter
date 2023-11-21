package org.yinqin.mqs.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.yinqin.mqs.common.Consts;
import org.yinqin.mqs.common.config.MqsProperties.AdapterProperties;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * kafka消费者
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.service.MessageConsumer
 * @since 1.0.0
 */
public class KafkaConsumer implements MessageConsumer {

    /**
     * kafka配置类
     */
    private final AdapterProperties kafkaProperties;

    /**
     * 批量消费处理器合集
     * key：topic
     * value：消息处理器
     */
    private final Map<String, MessageHandler> batchMessageHandlers;

    /**
     * 单条消费处理器合集
     * key：topic
     * value：消息处理器
     */
    private final Map<String, MessageHandler> messageHandlers;

    /**
     * 广播消费处理器合集
     * key：topic
     * value：消息处理器
     */
    private final Map<String, MessageHandler> broadcastHandlers;

    /**
     * 拉取消息工作线程集合
     */
    private final List<PollWorker> pollWorkerList = new ArrayList<>();

    /**
     * 异步线程组
     */
    private final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

    public KafkaConsumer(AdapterProperties kafkaProperties, Map<String, MessageHandler> batchMessageHandlers, Map<String, MessageHandler> transactionHandlers, Map<String, MessageHandler> broadcastHandlers) {
        this.kafkaProperties = kafkaProperties;
        this.batchMessageHandlers = batchMessageHandlers;
        this.messageHandlers = transactionHandlers;
        this.broadcastHandlers = broadcastHandlers;
        executor.setCorePoolSize(kafkaProperties.getKafka().getPollTaskConfig().getCorePoolSize());
        // 设置最大线程数
        executor.setMaxPoolSize(kafkaProperties.getKafka().getPollTaskConfig().getMaxPoolSize());
        // 设置队列容量
        executor.setQueueCapacity(kafkaProperties.getKafka().getPollTaskConfig().getQueueCapacity());
        // 设置线程活跃时间（秒）
        executor.setKeepAliveSeconds(kafkaProperties.getKafka().getPollTaskConfig().getKeepAliveSeconds());
        // 设置线程名称
        executor.setThreadNamePrefix("kafka-poll-thread");
        // 设置拒绝策略
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
    }

    /**
     * 启动所有类型的消费组
     */
    @Override
    public void start() {
        if (!messageHandlers.isEmpty()) {
            createConsumer(Consts.TRAN, messageHandlers);
        }
        if (!batchMessageHandlers.isEmpty()) {
            createConsumer(Consts.BATCH, batchMessageHandlers);
        }
        if (!broadcastHandlers.isEmpty()) {
            createConsumer(Consts.BROADCAST, broadcastHandlers);
        }
    }

    /**
     * 关闭所有源生kafka消费者
     * 停止所有拉取消息工作线程
     */
    @Override
    public void destroy() {
        for (PollWorker pollWorker : pollWorkerList) {
            pollWorker.shutdown();
        }
    }

    /**
     * 创建源生kafka消费者
     *
     * @param consumerType    消费组类型
     * @param messageHandlers 消费处理器合集
     */
    private void createConsumer(String consumerType, Map<String, MessageHandler> messageHandlers) {
        Properties properties = new Properties();
        properties.putAll(kafkaProperties.getKafka().getClientConfig());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        String groupName = kafkaProperties.getGroupName();
        if (consumerType.equals(Consts.BATCH)) groupName += "_BATCH";
        if (consumerType.equals(Consts.TRAN)) properties.setProperty("max.poll.records", "1");
        if (consumerType.equals(Consts.BROADCAST))
            groupName += "_BROADCAST_" + properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
        properties.setProperty("group.id", groupName);
        org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(messageHandlers.keySet());
        PollWorker pollWorker = new PollWorker(kafkaConsumer, messageHandlers,kafkaProperties.getKafka().getInterval());
        executor.execute(pollWorker);
        pollWorkerList.add(pollWorker);
    }

}
