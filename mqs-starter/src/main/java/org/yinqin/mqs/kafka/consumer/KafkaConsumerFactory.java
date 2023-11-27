package org.yinqin.mqs.kafka.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Consts;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.factory.ConsumerFactory;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.common.util.ConvertUtil;
import org.yinqin.mqs.kafka.PollWorker;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * kafka消费者工厂类
 *
 * @author YinQin
 * @createDate 2023年11月27日
 * @since 1.0.6
 * @see org.yinqin.mqs.common.factory.ConsumerFactory
 * @version 1.0.6
 */
public class KafkaConsumerFactory implements ConsumerFactory {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerFactory.class);
    @Override
    public MessageConsumer createTranConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        // 初始化配置
        Properties kafkaProperties = new Properties();
        init(kafkaProperties, properties);
        // 设置消费模式为单条消费
        kafkaProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "1");
        // 设置消费组名称
        String groupName = properties.getGroupName();
        groupName = ConvertUtil.convertName(groupName, properties.getGroup());
        kafkaProperties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, groupName);
        // 创建kafka原生消费者
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        // 订阅topic
        subscribe(kafkaConsumer, instanceId, groupName, messageHandlers);
        // 创建拉取消息工作线程
        PollWorker pollWorker = new PollWorker(kafkaConsumer, messageHandlers, properties.getKafka().getInterval());
        // 创建自定义消费者
        CustomKafkaConsumer consumer = new CustomKafkaConsumer(instanceId, pollWorker);
        consumer.start();
        return consumer;
    }

    @Override
    public MessageConsumer createBatchConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        // 初始化配置
        Properties kafkaProperties = new Properties();
        init(kafkaProperties, properties);
        // 设置消费组名称
        String groupName = properties.getGroupName() + Consts.BATCH_SUFFIX;
        groupName = ConvertUtil.convertName(groupName, properties.getGroup());
        kafkaProperties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, groupName);
        // 创建kafka原生消费者
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        // 订阅topic
        subscribe(kafkaConsumer, instanceId, groupName, messageHandlers);
        // 创建拉取消息工作线程
        PollWorker pollWorker = new PollWorker(kafkaConsumer, messageHandlers, properties.getKafka().getInterval());
        // 创建自定义消费者
        CustomKafkaConsumer consumer = new CustomKafkaConsumer(instanceId, pollWorker);
        consumer.start();
        return consumer;
    }

    @Override
    public MessageConsumer createBroadcastConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        // 初始化配置
        Properties kafkaProperties = new Properties();
        init(kafkaProperties, properties);
        // 设置消费组名称
        String groupName = properties.getGroupName();
        groupName += Consts.BROADCAST_CONNECTOR + kafkaProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
        groupName = ConvertUtil.convertName(groupName, properties.getGroup());
        kafkaProperties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, groupName);
        // 创建kafka原生消费者
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        // 订阅topic
        subscribe(kafkaConsumer, instanceId, groupName, messageHandlers);
        // 创建拉取消息工作线程
        PollWorker pollWorker = new PollWorker(kafkaConsumer, messageHandlers, properties.getKafka().getInterval());
        // 创建自定义消费者
        CustomKafkaConsumer consumer = new CustomKafkaConsumer(instanceId, pollWorker);
        consumer.start();
        return consumer;
    }

    /**
     * 初始化kafka配置
     * @param kafkaProperties kafka配置
     * @param properties mqs配置
     */
    private void init(Properties kafkaProperties, MqsProperties.AdapterProperties properties) {
        kafkaProperties.putAll(properties.getKafka().getClientConfig());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString().replace(Consts.HYPHEN, Consts.EMPTY).substring(0, 8));
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Consts.TRUE);
    }

    /**
     * 订阅topic
     * @param kafkaConsumer kafka消费者
     * @param instanceId 实例ID
     * @param groupName 消费组名称
     * @param messageHandlers 消息处理器
     */
    private void subscribe(KafkaConsumer<String, byte[]> kafkaConsumer, String instanceId, String groupName, Map<String, MessageHandler> messageHandlers) {
        kafkaConsumer.subscribe(messageHandlers.keySet());
        for (String topic : messageHandlers.keySet()) {
            logger.info("实例：{} 消费者启动中，消费组：{} 订阅Topic：{}", instanceId, groupName, topic);
        }
    }
}
