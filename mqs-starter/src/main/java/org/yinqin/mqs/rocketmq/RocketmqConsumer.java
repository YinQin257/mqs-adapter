package org.yinqin.mqs.rocketmq;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Consts;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.config.MqsProperties.RocketmqProperties;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.rocketmq.MessageListener.BatchMessageListener;
import org.yinqin.mqs.rocketmq.MessageListener.MessageListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * &#064;description: rocketmq消费者实现
 * &#064;author: YinQin
 * &#064;date: 2023-09-28 14:24
 */
public class RocketmqConsumer implements MessageConsumer {

    private final Logger logger = LoggerFactory.getLogger(RocketmqProducer.class);

    private final RocketmqProperties rocketmqProperties;

    private final Map<String, MessageHandler> messageHandlers;

    private final Map<String, MessageHandler> transactionHandlers;

    private final Map<String, MessageHandler> broadcastHandlers;

    private final List<DefaultMQPushConsumer> consumerList = new ArrayList<>();

    public RocketmqConsumer(RocketmqProperties rocketmqProperties, Map<String, MessageHandler> messageHandlers, Map<String, MessageHandler> transactionHandlers, Map<String, MessageHandler> broadcastHandlers) {
        this.rocketmqProperties = rocketmqProperties;
        this.messageHandlers = messageHandlers;
        this.transactionHandlers = transactionHandlers;
        this.broadcastHandlers = broadcastHandlers;
    }


    private DefaultMQPushConsumer createConsumer(String consumerType) throws MQClientException {
        DefaultMQPushConsumer consumer;
        if (rocketmqProperties.getAcl().isEnabled()) {
            consumer = new DefaultMQPushConsumer(rocketmqProperties.getGroupName(),new AclClientRPCHook(rocketmqProperties.getAcl()),new AllocateMessageQueueAveragely());
        } else {
            consumer = new DefaultMQPushConsumer(rocketmqProperties.getGroupName());
        }
        consumer.resetClientConfig(rocketmqProperties.getClientConfig());
        consumer.setInstanceName(UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        consumer.setConsumeMessageBatchMaxSize(rocketmqProperties.getConsumeMessageBatchMaxSize()); //公共消息可以配置每次消费数量,默认为1
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMax(rocketmqProperties.getConsumeThreadMax());
        consumer.setConsumeThreadMin(rocketmqProperties.getConsumeThreadMin());
        consumer.setPullThresholdForQueue(rocketmqProperties.getPullThresholdForQueue());
        consumer.setConsumeConcurrentlyMaxSpan(rocketmqProperties.getConsumeConcurrentlyMaxSpan());
        if (consumerType.equals(Consts.BATCH)) consumer.registerMessageListener(new BatchMessageListener(messageHandlers));
        if (consumerType.equals(Consts.TRAN)){
            consumer.registerMessageListener(new MessageListener(transactionHandlers));
            consumer.setConsumerGroup(rocketmqProperties.getGroupName()  + "_TRAN");
            consumer.setConsumeMessageBatchMaxSize(1);
        }
        if (consumerType.equals(Consts.BROADCAST)) {
            consumer.registerMessageListener(new MessageListener(broadcastHandlers));
            consumer.setConsumeMessageBatchMaxSize(1);
            consumer.setConsumerGroup(rocketmqProperties.getGroupName() + "_BROADCAST");
            consumer.setMessageModel(MessageModel.BROADCASTING);
        }
        for (String topic : messageHandlers.keySet()) {
            logger.info("rocketmq{}消费组{}启动中，订阅Topic：{}", consumerType, rocketmqProperties.getGroupName(), topic);
            consumer.subscribe(topic, "*");
        }
        consumer.start();
        logger.info("rocketmq{}消费组{}启动成功", consumerType, rocketmqProperties.getGroupName());
        return consumer;
    }

    @Override
    public void start() throws Exception {
        if (!messageHandlers.isEmpty()) {
            DefaultMQPushConsumer batch = createConsumer(Consts.BATCH);
            consumerList.add(batch);
        }
        if (!transactionHandlers.isEmpty()) {
            DefaultMQPushConsumer batch = createConsumer(Consts.TRAN);
            consumerList.add(batch);
        }
        if (!broadcastHandlers.isEmpty()) {
            DefaultMQPushConsumer batch = createConsumer(Consts.BROADCAST);
            consumerList.add(batch);
        }
    }

    @Override
    public void destroy() {
        consumerList.forEach(DefaultMQPushConsumer::shutdown);
    }


}
