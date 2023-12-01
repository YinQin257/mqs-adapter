package org.yinqin.mqs.common.factory;

import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.service.MessageConsumer;

import java.util.Map;

/**
 * 消费者工厂抽象接口
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年11月27日
 * @since 1.0.6
 */
public abstract class ConsumerFactory {

    /**
     * 启动消费者方法，消费者具体是什么类型由子类控制
     * @param instanceId 实例ID
     * @param properties 配置类
     * @param messageHandlers 消费者集合
     * @return  消费者实例
     */
    public MessageConsumer startConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers) {
        MessageConsumer consumer = createConsumer(instanceId, properties, messageHandlers);
        consumer.start();
        return consumer;
    }

    /**
     * 工厂方法
     *
     * @param instanceId 实例ID
     * @param properties 配置
     * @param messageHandlers 消息处理器
     * @return 单条消费者
     */
    public abstract MessageConsumer createConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers);

}
