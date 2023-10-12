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
import org.yinqin.mqs.common.config.MqsProperties.AdapterProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;
import org.yinqin.mqs.common.service.MessageProducer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author YinQin
 * @description rocketmq生产者
 * @createTime 2023-09-28 14:25
 */
public class RocketmqProducer implements MessageProducer {

    private final Logger logger = LoggerFactory.getLogger(RocketmqProducer.class);

    /**
     * rocketmq配置类
     */
    private final AdapterProperties rocketmqProperties;

    /**
     * 源生rocketmq生产者
     */
    private DefaultMQProducer producer;

    public RocketmqProducer(AdapterProperties rocketmqProperties) {
        this.rocketmqProperties = rocketmqProperties;
    }

    /**
     * 启动rocketmq生产者
     *
     * @throws Exception none
     */
    @Override
    public void start() throws Exception {
        logger.debug("rocketmq生产者启动中，启动配置：{}", rocketmqProperties.toString());
        if (rocketmqProperties.getRocketmq().getAcl().isEnabled()) {
            producer = new DefaultMQProducer(rocketmqProperties.getGroupName(), new AclClientRPCHook(rocketmqProperties.getRocketmq().getAcl()));
        } else {
            producer = new DefaultMQProducer(rocketmqProperties.getGroupName());
        }
        producer.resetClientConfig(rocketmqProperties.getRocketmq().getClientConfig());
        producer.setInstanceName(UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        producer.setAccessChannel(AccessChannel.CLOUD);
        producer.start();
    }

    /**
     * 同步发送消息方法
     *
     * @param adapterMessage 消息
     * @return 消息处理结果
     */
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
            logger.error("同步消息发送失败，失败原因：", e);
        }
        return messageSendResult;
    }

    /**
     * 同步发送消息方法
     *
     * @param adapterMessage 消息
     * @param timeout        同步等待时间
     * @param unit           时间单位
     * @return 消息处理结果
     */
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
            logger.error("同步消息发送失败，失败原因：", e);
        }
        return messageSendResult;
    }

    /**
     * 异步发送消息方法
     *
     * @param adapterMessage 消息
     * @param callback       消息发送结果回调
     */
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
            logger.error("异步消息发送失败，失败原因：", e);
        }
    }

    /**
     * 停止rocketmq生产者
     */
    @Override
    public void destroy() {
        producer.shutdown();
    }

    /**
     * 适配器消息转源生消息
     *
     * @param message 适配器消息
     * @return rocketmq源生消息
     */
    public Message AdapterMessageToMessage(AdapterMessage message) {
        return new Message(message.getTopic(), message.getTag(), message.getBizKey(), message.getBody());
    }
}
