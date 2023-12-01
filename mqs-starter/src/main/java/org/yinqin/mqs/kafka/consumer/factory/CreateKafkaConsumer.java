package org.yinqin.mqs.kafka.consumer.factory;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.util.ConvertUtil;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * 创建kafka消费者公共方法
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年11月30日
 * @since 1.0.6
 */
public interface CreateKafkaConsumer {

    Logger logger = LoggerFactory.getLogger(CreateKafkaConsumer.class);

    /**
     * 初始化kafka配置
     *
     * @param kafkaProperties kafka配置
     * @param properties      mqs配置
     */
    default void init(Properties kafkaProperties, MqsProperties.AdapterProperties properties) {
        kafkaProperties.putAll(properties.getKafka().getClientConfig());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString().replace(Constants.HYPHEN, Constants.EMPTY).substring(0, 8));
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Constants.TRUE);
    }

    /**
     * 订阅topic
     *
     * @param kafkaConsumer   kafka消费者
     * @param instanceId      实例ID
     * @param groupName       消费组名称
     * @param messageHandlers 消息处理器
     */
    default void subscribe(KafkaConsumer<String, byte[]> kafkaConsumer, String instanceId, String groupName, Map<String, MessageHandler> messageHandlers) {
        kafkaConsumer.subscribe(messageHandlers.keySet());
        for (String topic : messageHandlers.keySet()) {
            logger.info("实例：{} 消费者启动中，消费组：{}，订阅Topic：{}", instanceId, groupName, topic);
        }
    }

    /**
     * 创建kafka原生消费者
     *
     * @param groupName       消费组名称
     * @param properties      mqs配置
     * @param kafkaProperties kafka配置
     * @return kafka原生消费者
     */
    default KafkaConsumer<String, byte[]> createKafkaConsumer(String groupName, MqsProperties.AdapterProperties properties, Properties kafkaProperties) {
        groupName = ConvertUtil.convertName(groupName, properties.getGroup());
        kafkaProperties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, groupName);
        // 创建kafka原生消费者
        return new KafkaConsumer<>(kafkaProperties);
    }
}
