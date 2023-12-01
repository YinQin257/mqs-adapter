package org.yinqin.test.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;
import org.yinqin.mqs.common.service.MessageProducer;
import org.yinqin.mqs.common.manager.ProducerManager;

import java.nio.charset.StandardCharsets;

/**
 * 发送消息测试接口
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年11月20日
 * @since 1.0.0
 */
@RestController
public class MqAdapterTestController {
    private static final Logger logger = LoggerFactory.getLogger(MqAdapterTestController.class);

    @Autowired
    ProducerManager producerManager;

    /**
     * 发送消息
     *
     * @param topic    主题
     * @param pubCount 发送消息数量
     * @param pubMode  发送消息模式
     * @param instanceId  消息中间件实例ID
     */
    @GetMapping("/pubMessage")
    public void pubMessage(@RequestParam String topic, @RequestParam Integer pubCount, @RequestParam String pubMode,@RequestParam String instanceId) {
        switch (pubMode) {
            case MessagePubMode.SYNC:
                syncPubMessage(topic, pubCount,producerManager.get(instanceId));
                break;
            case MessagePubMode.ASYNC:
                asyncPubMessage(topic, pubCount,producerManager.get(instanceId));
                break;
            case MessagePubMode.ONE_WAY:
                oneWayPubMessage(topic, pubCount,producerManager.get(instanceId));
                break;
            default:
                logger.info("未支持的消息模式：{}", pubMode);
                break;
        }
    }

    /**
     * 发送单向消息
     *
     * @param topic    主题
     * @param pubCount 发送消息数量
     * @param producer 生产者
     */
    private void oneWayPubMessage(String topic, Integer pubCount,MessageProducer producer) {
        for (int i = 0; i < pubCount; i++){
            AdapterMessage message = AdapterMessage.builder().topic(topic).body("This is a one way message".getBytes(StandardCharsets.UTF_8)).build();
            producer.sendOneWay(message);
        }
    }

    /**
     * 发送异步消息
     *
     * @param topic    主题
     * @param pubCount 发送消息数量
     * @param producer 生产者
     */
    private void asyncPubMessage(String topic, Integer pubCount,MessageProducer producer) {
        for (int i = 0; i < pubCount; i++) {
            AdapterMessage message = AdapterMessage.builder().topic(topic).body("This is a async message".getBytes(StandardCharsets.UTF_8)).build();
            producer.sendMessage(message, new MessageCallback() {
                @Override
                public void onSuccess() {
                    logger.info("消息异步发送成功");
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.info("消息异步发送失败,原因:" + throwable.getMessage());
                }
            });
        }
    }

    /**
     * 发送同步消息
     *
     * @param topic    主题
     * @param pubCount 发送消息数量
     * @param producer 生产者
     */
    private void syncPubMessage(String topic, Integer pubCount,MessageProducer producer) {
        for (int i = 0; i < pubCount; i++) {
            AdapterMessage message = AdapterMessage.builder().topic(topic).body("This is a sync message".getBytes(StandardCharsets.UTF_8)).build();
            MessageSendResult send = producer.sendMessage(message);
            if (send.getStatus() == Constants.SUCCESS) logger.info("消息同步发送成功");
            else logger.info("消息同步发送失败,原因:" + send.getThrowable().getMessage());
        }
    }


}
