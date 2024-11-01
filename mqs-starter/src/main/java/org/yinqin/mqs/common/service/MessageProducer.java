package org.yinqin.mqs.common.service;

import org.springframework.beans.factory.DisposableBean;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;

import java.util.concurrent.TimeUnit;

/**
 * 消息适配器定义的顶级生产者接口
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @see DisposableBean
 * @since 1.0.0
 */
public interface MessageProducer extends DisposableBean {

    /**
     * 消息生产者启动方法
     */
    void start();

    /**
     * 同步发送消息方法
     *
     * @param message 消息
     * @return 消息发送结果
     */
    MessageSendResult sendMessage(AdapterMessage message);

    /**
     * 同步发送消息方法
     *
     * @param message 消息
     * @param timeout 同步等待时间
     * @param unit    时间单位
     * @return 消息发送结果
     */
    MessageSendResult sendMessage(AdapterMessage message, long timeout, TimeUnit unit);

    /**
     * 异步发送消息方法
     *
     * @param message  消息
     * @param callback 消息发送结果回调
     */
    void sendMessage(AdapterMessage message, MessageCallback callback);

    /**
     * 单向发送消息方法
     *
     * @param message 消息
     */
    default void sendOneWay(AdapterMessage message) {
        sendMessage(message, null);
    }

}
