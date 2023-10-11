package org.yinqin.mqs.rocketmq;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Consts;
import org.yinqin.mqs.common.config.MqsProperties.RocketmqProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;
import org.yinqin.mqs.common.service.MessageProducer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * &#064;description: rocketmq生产者实现
 * &#064;author: YinQin
 * &#064;date: 2023-09-28 14:25
 */
public class RocketmqProducer implements MessageProducer {

    private final Logger logger = LoggerFactory.getLogger(RocketmqProducer.class);

    private final RocketmqProperties rocketmqProperties;

    private DefaultMQProducer producer;

    public RocketmqProducer (RocketmqProperties rocketmqProperties) {
        this.rocketmqProperties = rocketmqProperties;
    }

    @Override
    public void start() throws Exception {
        logger.debug("rocketmq生产者启动中，启动配置：{}", rocketmqProperties.toString());
        if (rocketmqProperties.getAcl().isEnabled()) {
            producer = new DefaultMQProducer(rocketmqProperties.getGroupName(),new AclClientRPCHook(rocketmqProperties.getAcl()));
        } else {
            producer = new DefaultMQProducer(rocketmqProperties.getGroupName());
        }
        producer.resetClientConfig(rocketmqProperties.getClientConfig());
        producer.setInstanceName(UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        producer.setAccessChannel(AccessChannel.CLOUD);
        producer.start();
    }

    @Override
    public MessageSendResult sendMessage(AdapterMessage adapterMessage) {
        Message message = AdapterMessageToMessage(adapterMessage);

        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            SendResult sendResult = producer.send(message);
            adapterMessage.setMsgId(sendResult.getMsgId());
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                messageSendResult.setStatus(Consts.SUCCESS);
            } else {
                messageSendResult.setStatus(Consts.ERROR);
                messageSendResult.setThrowable(new MQClientException(0, sendResult.getSendStatus().name()));
            }
        } catch (Exception e) {
            messageSendResult.setStatus(Consts.ERROR);
            messageSendResult.setThrowable(e);
            logger.error("同步消息发送失败，失败原因：",e);
        }
        return messageSendResult;
    }

    @Override
    public MessageSendResult sendMessage(AdapterMessage adapterMessage, long timeout, TimeUnit unit) {
        Message message = AdapterMessageToMessage(adapterMessage);
        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            SendResult sendResult = producer.send(message, unit.toMillis(timeout));
            adapterMessage.setMsgId(sendResult.getMsgId());
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                messageSendResult.setStatus(Consts.SUCCESS);
            } else {
                messageSendResult.setStatus(Consts.ERROR);
                messageSendResult.setThrowable(new MQClientException(0, sendResult.getSendStatus().name()));
            }
        } catch (Exception e) {
            messageSendResult.setStatus(Consts.ERROR);
            messageSendResult.setThrowable(e);
            logger.error("同步消息发送失败，失败原因：",e);
        }
        return messageSendResult;
    }

    @Override
    public void sendMessage(AdapterMessage adapterMessage, MessageCallback callback) {
        Message message = AdapterMessageToMessage(adapterMessage);
        try {
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    adapterMessage.setMsgId(sendResult.getMsgId());
                    if (callback != null) callback.onSuccess();
                }

                @Override
                public void onException(Throwable e) {
                    if (callback != null) callback.onError(e);
                }
            });
        } catch (Exception e) {
            logger.error("异步消息发送失败，失败原因：",e);
        }
    }

    @Override
    public void destroy() {
        producer.shutdown();
    }

    public Message AdapterMessageToMessage(AdapterMessage message) {
        return new Message(message.getTopic(), message.getTag(), message.getBizKey(), message.getBody());
    }
}