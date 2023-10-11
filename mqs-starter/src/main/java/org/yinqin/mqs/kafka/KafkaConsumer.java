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
 * @description kafka消费者
 * @author YinQin
 * @createTime 2023-10-10 15:19
 */
public class KafkaConsumer implements MessageConsumer {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    /**
     * kafka配置类
     */
    private final KafkaProperties kafkaProperties;

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
     * 源生kafka消费者合集
     */
    private final List<org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]>> consumerList = new ArrayList<>();

    /**
     * 拉取消息工作线程集合
     */
    private final List<PollWorker> pollWorkerList = new ArrayList<>();

    /**
     * 异步线程组
     */
    private final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

    public KafkaConsumer(KafkaProperties kafkaProperties, Map<String, MessageHandler> batchMessageHandlers, Map<String, MessageHandler> transactionHandlers, Map<String, MessageHandler> broadcastHandlers) {
        this.kafkaProperties = kafkaProperties;
        this.batchMessageHandlers = batchMessageHandlers;
        this.messageHandlers = transactionHandlers;
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

    /**
     * 启动所有类型的消费组
     * @throws Exception none
     */
    @Override
    public void start() throws Exception {
        if (!batchMessageHandlers.isEmpty()) {
            consumerList.add(createConsumer(Consts.BATCH));
        }
        if (!messageHandlers.isEmpty()) {
            consumerList.add(createConsumer(Consts.TRAN));
        }
        if (!broadcastHandlers.isEmpty()) {
            consumerList.add(createConsumer(Consts.BROADCAST));
        }
    }

    /**
     * 关闭所有源生kafka消费者
     * 停止所有拉取消息工作线程
     * @throws Exception none
     */
    @Override
    public void destroy() throws Exception {
        consumerList.forEach(org.apache.kafka.clients.consumer.KafkaConsumer::close);
        pollWorkerList.forEach(PollWorker::shutdown);
    }

    /**
     * 创建源生kafka消费者
     * @param consumerType 消费组类型
     * @return 源生kafka消费者
     */
    private org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> createConsumer(String consumerType) {
        Properties properties = kafkaProperties.getClientConfig();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        Set<String> topicNames = new HashSet<>();
        PollWorker pollWorker = new PollWorker();
        org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> kafkaConsumer;
        if (consumerType.equals(Consts.BATCH)) {
            properties.setProperty("group.id", kafkaProperties.getGroupName() + "_BATCH");
            topicNames = batchMessageHandlers.keySet();
            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
            pollWorker = new PollWorker(consumerType, kafkaConsumer, batchMessageHandlers);
        } else if (consumerType.equals(Consts.TRAN)) {
            properties.setProperty("max.poll.records", "1");
            properties.setProperty("group.id", kafkaProperties.getGroupName());
            topicNames = messageHandlers.keySet();
            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
            pollWorker = new PollWorker(consumerType, kafkaConsumer, messageHandlers);
        } else {
            properties.setProperty("max.poll.records", "1");
            properties.setProperty("group.id", kafkaProperties.getGroupName() + "_BROADCAST_" + properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
            topicNames = broadcastHandlers.keySet();
            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
            pollWorker = new PollWorker(consumerType, kafkaConsumer, broadcastHandlers);
        }
        kafkaConsumer.subscribe(topicNames);
        executor.execute(pollWorker);
        pollWorkerList.add(pollWorker);
        return kafkaConsumer;
    }

}
