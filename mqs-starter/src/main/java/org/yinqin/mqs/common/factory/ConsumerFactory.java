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
public interface ConsumerFactory {

    /**
     * 创建单条消费者
     *
     * @param instanceId      实例ID
     * @param properties      配置
     * @param messageHandlers 消息处理器
     * @return 单条消费者
     */
    MessageConsumer createTranConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers);

    /**
     * 创建批量消费者
     *
     * @param instanceId      实例ID
     * @param properties      配置
     * @param messageHandlers 消息处理器
     * @return 批量消费者
     */
    MessageConsumer createBatchConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers);

    /**
     * 创建广播消费者
     *
     * @param instanceId      实例ID
     * @param properties      配置
     * @param messageHandlers 消息处理器
     * @return 广播消费者
     */
    MessageConsumer createBroadcastConsumer(String instanceId, MqsProperties.AdapterProperties properties, Map<String, MessageHandler> messageHandlers);
}
