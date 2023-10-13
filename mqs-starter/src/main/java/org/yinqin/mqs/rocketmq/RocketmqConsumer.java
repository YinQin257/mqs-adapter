package org.yinqin.mqs.rocketmq;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Consts;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.config.MqsProperties.AdapterProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * rocketmq消费者
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.service.MessageConsumer
 * @since 1.0.0
 */
public class RocketmqConsumer implements MessageConsumer {

    private final Logger logger = LoggerFactory.getLogger(RocketmqProducer.class);

    /**
     * rocketmq配置类
     */
    private final AdapterProperties rocketmqProperties;

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
     * 源生rocketmq消费者合集
     */
    private final List<DefaultMQPushConsumer> consumerList = new ArrayList<>();

    public RocketmqConsumer(AdapterProperties rocketmqProperties, Map<String, MessageHandler> batchMessageHandlers, Map<String, MessageHandler> messageHandlers, Map<String, MessageHandler> broadcastHandlers) {
        this.rocketmqProperties = rocketmqProperties;
        this.batchMessageHandlers = batchMessageHandlers;
        this.messageHandlers = messageHandlers;
        this.broadcastHandlers = broadcastHandlers;
    }

    /**
     * 创建源生rocketmq消费者
     *
     * @param consumerType 消费者类型
     * @return 源生rocketmq消费者
     * @throws MQClientException none
     */
    private DefaultMQPushConsumer createConsumer(String consumerType, Map<String, MessageHandler> messageHandlers) throws MQClientException {
        DefaultMQPushConsumer consumer;
        if (rocketmqProperties.getRocketmq().getAcl().isEnabled()) {
            consumer = new DefaultMQPushConsumer(rocketmqProperties.getGroupName(), new AclClientRPCHook(rocketmqProperties.getRocketmq().getAcl()), new AllocateMessageQueueAveragely());
        } else {
            consumer = new DefaultMQPushConsumer(rocketmqProperties.getGroupName());
        }
        consumer.resetClientConfig(rocketmqProperties.getRocketmq().getClientConfig());
        consumer.setInstanceName(UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        consumer.setConsumeMessageBatchMaxSize(rocketmqProperties.getRocketmq().getConsumeMessageBatchMaxSize()); //公共消息可以配置每次消费数量,默认为1
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMax(rocketmqProperties.getRocketmq().getConsumeThreadMax());
        consumer.setConsumeThreadMin(rocketmqProperties.getRocketmq().getConsumeThreadMin());
        consumer.setPullThresholdForQueue(rocketmqProperties.getRocketmq().getPullThresholdForQueue());
        consumer.setConsumeConcurrentlyMaxSpan(rocketmqProperties.getRocketmq().getConsumeConcurrentlyMaxSpan());
        String groupName = rocketmqProperties.getGroupName();
        if (consumerType.equals(Consts.TRAN)) consumer.setConsumeMessageBatchMaxSize(1);
        if (consumerType.equals(Consts.BATCH)) groupName += "_BATCH";
        if (consumerType.equals(Consts.BROADCAST)) {
            groupName += "_BROADCAST";
            consumer.setMessageModel(MessageModel.BROADCASTING);
        }
        consumer.setConsumerGroup(groupName);
        for (String topic : messageHandlers.keySet()) {
            logger.info("rocketmq{}消费组{}启动中，订阅Topic：{}", consumerType, rocketmqProperties.getGroupName(), topic);
            consumer.subscribe(topic, "*");
        }
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            if (msgs.isEmpty()) return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            MessageExt firstMsg = msgs.get(0); //每次只拉取一条
            List<AdapterMessage> messages = new ArrayList<>();
            msgs.forEach(msg -> {
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

        consumer.start();
        logger.info("rocketmq{}消费组{}启动成功", consumerType, rocketmqProperties.getGroupName());
        return consumer;
    }

    /**
     * 启动所有类型的消费组
     *
     * @throws Exception none
     */
    @Override
    public void start() throws Exception {
        if (!messageHandlers.isEmpty()) {
            DefaultMQPushConsumer batch = createConsumer(Consts.TRAN, messageHandlers);
            consumerList.add(batch);
        }
        if (!batchMessageHandlers.isEmpty()) {
            DefaultMQPushConsumer batch = createConsumer(Consts.BATCH, batchMessageHandlers);
            consumerList.add(batch);
        }
        if (!broadcastHandlers.isEmpty()) {
            DefaultMQPushConsumer batch = createConsumer(Consts.BROADCAST, broadcastHandlers);
            consumerList.add(batch);
        }
    }

    /**
     * 关闭所有源生rocketmq消费者
     */
    @Override
    public void destroy() {
        consumerList.forEach(DefaultMQPushConsumer::shutdown);
    }


}
