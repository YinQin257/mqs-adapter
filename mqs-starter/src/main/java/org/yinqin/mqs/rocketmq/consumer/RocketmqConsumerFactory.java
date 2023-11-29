package org.yinqin.mqs.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.factory.ConsumerFactory;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.common.util.ConvertUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * rocketmq消费者工厂类
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年11月27日
 * @see org.yinqin.mqs.common.factory.ConsumerFactory
 * @since 1.0.6
 */
public class RocketmqConsumerFactory extends ConsumerFactory {

    private final Logger logger = LoggerFactory.getLogger(RocketmqConsumerFactory.class);

    @Override
    public MessageConsumer createTranConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        CustomRocketmqConsumer consumer = new CustomRocketmqConsumer(instanceId, properties);
        init(consumer.getConsumer(), properties);
        consumer.getConsumer().setConsumeMessageBatchMaxSize(1);
        String groupName = ConvertUtil.convertName(properties.getGroupName(), properties.getGroup());
        consumer.getConsumer().setConsumerGroup(groupName);
        subscribe(consumer.getConsumer(), instanceId, groupName, messageHandlers);
        registerMessageListener(consumer.getConsumer(), messageHandlers);
        consumer.start();
        return consumer;
    }

    @Override
    public MessageConsumer createBatchConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        CustomRocketmqConsumer consumer = new CustomRocketmqConsumer(instanceId, properties);
        init(consumer.getConsumer(), properties);
        String groupName = ConvertUtil.convertName(properties.getGroupName() + Constants.BATCH_SUFFIX, properties.getGroup());
        consumer.getConsumer().setConsumerGroup(groupName);
        subscribe(consumer.getConsumer(), instanceId, groupName, messageHandlers);
        registerMessageListener(consumer.getConsumer(), messageHandlers);
        consumer.start();
        return consumer;
    }

    @Override
    public MessageConsumer createBroadcastConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        CustomRocketmqConsumer consumer = new CustomRocketmqConsumer(instanceId, properties);
        init(consumer.getConsumer(), properties);
        String groupName = ConvertUtil.convertName(properties.getGroupName() + Constants.BROADCAST_SUFFIX, properties.getGroup());
        consumer.getConsumer().setConsumerGroup(groupName);
        consumer.getConsumer().setMessageModel(MessageModel.BROADCASTING);
        subscribe(consumer.getConsumer(), instanceId, groupName, messageHandlers);
        registerMessageListener(consumer.getConsumer(), messageHandlers);
        consumer.start();
        return consumer;
    }

    /**
     * 初始化配置
     *
     * @param consumer   源生rocketmq消费者
     * @param properties 配置
     */
    private void init(DefaultMQPushConsumer consumer, MqsProperties.AdapterProperties properties) {
        consumer.resetClientConfig(properties.getRocketmq().getClientConfig());
        consumer.setInstanceName(UUID.randomUUID().toString().replace(Constants.HYPHEN, Constants.EMPTY).substring(0, 8));
        consumer.setConsumeMessageBatchMaxSize(properties.getRocketmq().getConsumeMessageBatchMaxSize()); //公共消息可以配置每次消费数量,默认为1
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMax(properties.getRocketmq().getConsumeThreadMax());
        consumer.setConsumeThreadMin(properties.getRocketmq().getConsumeThreadMin());
        consumer.setPullThresholdForQueue(properties.getRocketmq().getPullThresholdForQueue());
        consumer.setConsumeConcurrentlyMaxSpan(properties.getRocketmq().getConsumeConcurrentlyMaxSpan());
    }

    /**
     * 订阅topic
     *
     * @param consumer        源生rocketmq消费者
     * @param instanceId      实例ID
     * @param groupName       消费组名称
     * @param messageHandlers 消息处理器
     */
    private void subscribe(DefaultMQPushConsumer consumer, String instanceId, String groupName, Map<String, MessageHandler> messageHandlers) {
        for (String topic : messageHandlers.keySet()) {
            logger.info("实例：{} 消费者启动中，消费组：{}，订阅Topic：{}", instanceId, groupName, topic);
            try {
                consumer.subscribe(topic, Constants.WILDCARD);
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 注册消息监听器
     *
     * @param consumer        源生rocketmq消费者
     * @param messageHandlers 消息处理器
     */
    private void registerMessageListener(DefaultMQPushConsumer consumer, Map<String, MessageHandler> messageHandlers) {
        consumer.registerMessageListener((MessageListenerConcurrently) (messageList, context) -> {
            if (messageList.isEmpty()) return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            MessageExt firstMsg = messageList.get(0); //每次只拉取一条
            List<AdapterMessage> messages = new ArrayList<>();
            messageList.forEach(msg -> {
                AdapterMessage message = AdapterMessage.builder()
                        .topic(msg.getTopic())
                        .tag(msg.getTags())
                        .bizKey(msg.getKeys())
                        .body(msg.getBody())
                        .msgId(msg.getMsgId())
                        .originMessage(msg)
                        .consumeTimes(msg.getReconsumeTimes())
                        .build();
                message.setOriginMessage(msg);
                message.setMsgId(msg.getMsgId());
                message.setConsumeTimes(msg.getReconsumeTimes());
                messages.add(message);
            });
            try {
                MessageAdapter messageAdapter = messageHandlers.get(firstMsg.getTopic()).getClass().getAnnotation(MessageAdapter.class);
                if (messageAdapter.isBatch()) {
                    messageHandlers.get(firstMsg.getTopic()).process(messages);
                } else {
                    for (AdapterMessage msg : messages) messageHandlers.get(firstMsg.getTopic()).process(msg);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (Exception e) {
                logger.error("主题{}消费异常：", firstMsg.getTopic(), e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
    }


}
