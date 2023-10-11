package org.yinqin.mqs.common.service;

import org.springframework.beans.factory.DisposableBean;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;

import java.util.concurrent.TimeUnit;

/**
 * &#064;description: 消息生产者SPI
 * &#064;author: YinQin
 * &#064;date: 2023-09-28 11:44
 */
public interface MessageProducer extends DisposableBean {

    public void start() throws Exception;

    public MessageSendResult sendMessage(AdapterMessage message);

    public MessageSendResult sendMessage(AdapterMessage message, long timeout, TimeUnit unit);

    public void sendMessage(AdapterMessage message, MessageCallback callback);

    default void sendOneWay(AdapterMessage message){
        sendMessage(message,null);
    }

}
