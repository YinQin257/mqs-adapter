package org.yinqin.mqs.kafka.consumer.factory;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.factory.ConsumerFactory;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.kafka.PollWorker;
import org.yinqin.mqs.kafka.consumer.CustomKafkaConsumer;

import java.util.Map;
import java.util.Properties;

/**
 * kafka广播消费者工厂类
 *
 * @author YinQin
 * @createDate 2023年11月27日
 * @since 1.0.6
 * @see ConsumerFactory
 * @version 1.0.6
 */
public class KafkaBroadcastConsumerFactory extends ConsumerFactory implements CreateKafkaConsumer {
    @Override
    public MessageConsumer createConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        Properties kafkaProperties = new Properties();
        init(kafkaProperties, properties);
        // 设置消费组名称
        String groupName = properties.getGroupName();
        groupName += Constants.BROADCAST_CONNECTOR + kafkaProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
        // 创建kafka原生消费者
        KafkaConsumer<String, byte[]> kafkaConsumer = createKafkaConsumer(groupName, properties, kafkaProperties);
        // 订阅topic
        subscribe(kafkaConsumer, instanceId, groupName, messageHandlers);
        // 创建拉取消息工作线程
        PollWorker pollWorker = new PollWorker(kafkaConsumer, messageHandlers, properties.getKafka().getInterval());
        // 创建自定义消费者
        return new CustomKafkaConsumer(instanceId, Constants.BROADCAST, pollWorker);
    }

}
