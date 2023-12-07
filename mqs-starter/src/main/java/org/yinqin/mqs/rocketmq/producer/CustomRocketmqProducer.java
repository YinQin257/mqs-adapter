package org.yinqin.mqs.rocketmq.producer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.config.MqsProperties.AdapterProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;
import org.yinqin.mqs.common.service.MessageProducer;
import org.yinqin.mqs.common.util.ConvertUtil;

import java.util.List;
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

    private static final String SYNC_SEND_ERROR_MESSAGE = "同步消息发送失败，失败原因：";

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
        logger.info("实例：{} 生产者创建中，创建配置：{}", instanceId, rocketmqProperties);
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
        Message message = ConvertUtil.adapterMessageToRocketmqMessage(adapterMessage, rocketmqProperties.getTopic());

        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            String bizKey = adapterMessage.getBizKey();
            SendResult sendResult;
            if (StringUtils.isNotBlank(bizKey) && StringUtils.isNumeric(bizKey)) {
                sendResult = producer.send(message, new CustomMessageQueueSelector(),bizKey);
            } else {
                sendResult = producer.send(message);
            }
            adapterMessage.setMsgId(sendResult.getMsgId());
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                messageSendResult.setStatus(Constants.SUCCESS);
            } else {
                messageSendResult.setStatus(Constants.ERROR);
                messageSendResult.setThrowable(new MQClientException(0, sendResult.getSendStatus().name()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
            messageSendResult.setStatus(Constants.ERROR);
            messageSendResult.setThrowable(e);
            logger.error(SYNC_SEND_ERROR_MESSAGE, e);
        } catch (Exception e) {
            messageSendResult.setStatus(Constants.ERROR);
            messageSendResult.setThrowable(e);
            logger.error(SYNC_SEND_ERROR_MESSAGE, e);
        }
        return messageSendResult;
    }

    private static class CustomMessageQueueSelector implements MessageQueueSelector {
        @Override
        public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
            int id = (o.hashCode() & 0x7FFFFFFF) % 100 + 1;
            int index = id % list.size();
            return list.get(index);
        }
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
        Message message = ConvertUtil.adapterMessageToRocketmqMessage(adapterMessage, rocketmqProperties.getTopic());
        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            String bizKey = adapterMessage.getBizKey();
            SendResult sendResult;
            if (StringUtils.isNotBlank(bizKey) && StringUtils.isNumeric(bizKey)) {
                sendResult = producer.send(message, new CustomMessageQueueSelector() ,bizKey,unit.toMillis(timeout));
            } else {
                sendResult = producer.send(message, unit.toMillis(timeout));
            }
            adapterMessage.setMsgId(sendResult.getMsgId());
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                messageSendResult.setStatus(Constants.SUCCESS);
            } else {
                messageSendResult.setStatus(Constants.ERROR);
                messageSendResult.setThrowable(new MQClientException(0, sendResult.getSendStatus().name()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
            messageSendResult.setStatus(Constants.ERROR);
            messageSendResult.setThrowable(e);
            logger.error(SYNC_SEND_ERROR_MESSAGE, e);
        } catch (Exception e) {
            messageSendResult.setStatus(Constants.ERROR);
            messageSendResult.setThrowable(e);
            logger.error(SYNC_SEND_ERROR_MESSAGE, e);
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
        Message message = ConvertUtil.adapterMessageToRocketmqMessage(adapterMessage, rocketmqProperties.getTopic());
        try {
            String bizKey = adapterMessage.getBizKey();
            SendCallback sendCallback = new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    adapterMessage.setMsgId(sendResult.getMsgId());
                    if (callback != null) callback.onSuccess();
                }

                @Override
                public void onException(Throwable e) {
                    if (callback != null) callback.onError(e);
                }
            };
            if (StringUtils.isNotBlank(bizKey) && StringUtils.isNumeric(bizKey)) {
                producer.send(message, new CustomMessageQueueSelector() ,bizKey,sendCallback);
            } else {
                producer.send(message, sendCallback);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
            logger.error("异步消息发送失败，失败原因：", e);
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
