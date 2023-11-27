package org.yinqin.mqs.rocketmq.producer;

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
import org.yinqin.mqs.common.util.ConvertUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 自定义rocketmq生产者
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.service.MessageConsumer
 * @since 1.0.0
 */
public class CustomRocketmqProducer implements MessageProducer {

    private final Logger logger = LoggerFactory.getLogger(CustomRocketmqProducer.class);

    /**
     * 实例ID
     */
    private final String instanceId;

    /**
     * rocketmq配置类
     */
    private final AdapterProperties rocketmqProperties;

    /**
     * 源生rocketmq生产者
     */
    private final DefaultMQProducer producer;

    public CustomRocketmqProducer(String instanceId, AdapterProperties rocketmqProperties) {
        this.instanceId = instanceId;
        this.rocketmqProperties = rocketmqProperties;
        logger.info("实例：{} 生产者创建中，创建配置：{}", instanceId, rocketmqProperties.toString());
        String groupName = ConvertUtil.convertName(rocketmqProperties.getGroupName(), rocketmqProperties.getGroup());
        if (rocketmqProperties.getRocketmq().getAcl().isEnabled()) {
            producer = new DefaultMQProducer(groupName, new AclClientRPCHook(rocketmqProperties.getRocketmq().getAcl()));
        } else {
            producer = new DefaultMQProducer(groupName);
        }
        producer.resetClientConfig(rocketmqProperties.getRocketmq().getClientConfig());
        producer.setInstanceName(UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        producer.setAccessChannel(AccessChannel.CLOUD);
    }

    /**
     * 启动rocketmq生产者
     */
    @Override
    public void start() {
        try {
            producer.start();
        } catch (MQClientException e) {
            logger.error("实例：{} 生产者启动失败", instanceId, e);
        }
        logger.info("实例：{} 生产者启动成功", instanceId);
    }

    /**
     * 同步发送消息方法
     *
     * @param adapterMessage 消息
     * @return 消息处理结果
     */
    @Override
    public MessageSendResult sendMessage(AdapterMessage adapterMessage) {
        Message message = ConvertUtil.AdapterMessageToRocketmqMessage(adapterMessage, rocketmqProperties.getTopic());

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
        Message message = ConvertUtil.AdapterMessageToRocketmqMessage(adapterMessage, rocketmqProperties.getTopic());
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
        Message message = ConvertUtil.AdapterMessageToRocketmqMessage(adapterMessage, rocketmqProperties.getTopic());
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
        logger.info("实例：{} 生产者停止成功", instanceId);
    }

}
