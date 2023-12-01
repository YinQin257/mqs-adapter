package org.yinqin.mqs.rocketmq.consumer.factory;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 创建rocketmq消费者公共方法
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年11月30日
 * @since 1.0.6
 */
public interface CreateRocketmqConsumer {

    Logger logger = LoggerFactory.getLogger(CreateRocketmqConsumer.class);

    /**
     * 初始化配置
     *
     * @param consumer   源生rocketmq消费者
     * @param properties 配置
     */
    default void init(DefaultMQPushConsumer consumer, MqsProperties.AdapterProperties properties) {
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
    default void subscribe(DefaultMQPushConsumer consumer, String instanceId, String groupName, Map<String, MessageHandler> messageHandlers) {
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
     * 注册多线程消息监听器
     *
     * @param consumer        源生rocketmq消费者
     * @param messageHandlers 消息处理器
     */
    default void registerMessageListenerConcurrently(DefaultMQPushConsumer consumer, Map<String, MessageHandler> messageHandlers) {
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

    /**
     * 注册顺序消息监听器
     *
     * @param consumer        源生rocketmq消费者
     * @param messageHandlers 消息处理器
     */
    default void registerMessageListenerOrderly(DefaultMQPushConsumer consumer, Map<String, MessageHandler> messageHandlers) {
        consumer.registerMessageListener((MessageListenerOrderly) (messageList, context) -> {
            if (messageList.isEmpty()) return ConsumeOrderlyStatus.SUCCESS;
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
                return ConsumeOrderlyStatus.SUCCESS;
            } catch (Exception e) {
                logger.error("主题{}消费异常：", firstMsg.getTopic(), e);
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });
    }
}
