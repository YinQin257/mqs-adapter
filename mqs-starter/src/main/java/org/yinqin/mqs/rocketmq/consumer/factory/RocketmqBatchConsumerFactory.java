package org.yinqin.mqs.rocketmq.consumer.factory;

import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.factory.ConsumerFactory;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.common.util.ConvertUtil;
import org.yinqin.mqs.rocketmq.consumer.CustomRocketmqConsumer;

import java.util.Map;

/**
 * rocketmq批量消费者工厂类
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年11月27日
 * @see ConsumerFactory
 * @since 1.0.6
 */
public class RocketmqBatchConsumerFactory extends ConsumerFactory implements CreateRocketmqConsumer {

    @Override
    public MessageConsumer createConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        CustomRocketmqConsumer consumer = new CustomRocketmqConsumer(instanceId, properties);
        init(consumer.getConsumer(), properties);
        String groupName = ConvertUtil.convertName(properties.getGroupName() + Constants.BATCH_SUFFIX, properties.getGroup());
        consumer.getConsumer().setConsumerGroup(groupName);
        subscribe(consumer.getConsumer(), instanceId, groupName, messageHandlers);
        registerMessageListenerConcurrently(consumer.getConsumer(), messageHandlers);
        return consumer;
    }
}
