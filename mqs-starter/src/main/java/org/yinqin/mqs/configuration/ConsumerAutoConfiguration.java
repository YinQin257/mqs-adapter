package org.yinqin.mqs.configuration;


import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.manager.ConsumerManager;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.common.util.ConvertUtil;
import org.yinqin.mqs.kafka.consumer.factory.KafkaBatchConsumerFactory;
import org.yinqin.mqs.kafka.consumer.factory.KafkaBroadcastConsumerFactory;
import org.yinqin.mqs.kafka.consumer.factory.KafkaTranConsumerFactory;
import org.yinqin.mqs.rocketmq.consumer.factory.RocketmqBatchConsumerFactory;
import org.yinqin.mqs.rocketmq.consumer.factory.RocketmqBroadcastConsumerFactory;
import org.yinqin.mqs.rocketmq.consumer.factory.RocketmqTranConsumerFactory;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * 消息适配器消费者自动装配类
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年10月13日
 * @see InitializingBean
 * @see DisposableBean
 * @see ApplicationContextAware
 * @see PriorityOrdered
 * @since 1.0.0
 */
@Configuration
public class ConsumerAutoConfiguration implements InitializingBean, DisposableBean, ApplicationContextAware, PriorityOrdered {

    private final Logger logger = LoggerFactory.getLogger(ConsumerAutoConfiguration.class);

    private ApplicationContext applicationContext;

    private final ConsumerManager consumerManager = new ConsumerManager();

    @Resource
    MqsProperties properties;

    /**
     * 注销consumer
     */
    @Override
    public void destroy() {
        for (MessageConsumer consumer : consumerManager) {
            try {
                consumer.destroy();
            } catch (Exception e) {
                logger.error("注销消费组失败：", e);
            }
        }
    }

    /**
     * 实现InitializingBean接口
     * 启动所有的消费组
     */
    @Override
    public void afterPropertiesSet() {
        Map<String, MessageHandler> messageHandlerBeans = applicationContext.getBeansOfType(MessageHandler.class);
        properties.getAdapter().forEach((instanceId, config) -> startConsumersForInstance(instanceId, config, messageHandlerBeans));
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    /**
     * 启动所有消费者实例
     * @param instanceId 实例ID
     * @param config 实例配置
     * @param messageHandlerBeans 消息处理器实现类集合
     */
    private void startConsumersForInstance(String instanceId, MqsProperties.AdapterProperties config,Map<String, MessageHandler> messageHandlerBeans) {
        // 未开启消费者，直接终止
        if (!config.isConsumerEnabled()) return;
        // 校验消费者实例配置
        validateConfig(instanceId, config);
        // 过滤出事务消息处理器
        Map<String, MessageHandler> messageHandlers = filterMessageHandlers(instanceId, config.getTopic(), messageHandlerBeans, messageAdapter -> !messageAdapter.isBatch() && !messageAdapter.isBroadcast());
        // 过滤出批量消息处理器
        Map<String, MessageHandler> batchMessageHandlers = filterMessageHandlers(instanceId, config.getTopic(), messageHandlerBeans, MessageAdapter::isBatch);
        // 过滤出广播消息处理器
        Map<String, MessageHandler> broadcastHandlers = filterMessageHandlers(instanceId, config.getTopic(), messageHandlerBeans,MessageAdapter::isBroadcast);
        // 启动所有消费者实例
        startConsumers(instanceId, config, messageHandlers, batchMessageHandlers, broadcastHandlers);
    }

    private void validateConfig(String instanceId, MqsProperties.AdapterProperties config) {
        if (StringUtils.isBlank(config.getVendorName())) {
            logger.error("实例：{}，消费者启动失败,vendorName不能为空", instanceId);
            throw new IllegalArgumentException("vendorName 不能为空");
        }
        if (StringUtils.isBlank(config.getGroupName())) {
            logger.error("实例：{}，消费者启动失败,groupName不能为空", instanceId);
            throw new IllegalArgumentException("groupName 不能为空");
        }
    }

    /**
     * 过滤所需的消息处理器
     * @param instanceId 实例ID
     * @param config 实例配置
     * @param messageHandlerBeans 消息处理器实现类集合
     * @param predicate 断言
     * @return 消息处理器集合
     */
    private Map<String, MessageHandler> filterMessageHandlers(String instanceId,
                                                                   MqsProperties.AdapterProperties.ConvertProperties config,
                                                                   Map<String, MessageHandler> messageHandlerBeans,
                                                                   Predicate<MessageAdapter> predicate) {
        Map<String, MessageHandler> messageHandlers = new HashMap<>();
        messageHandlerBeans.forEach((beanName, bean) -> {
            MessageAdapter messageAdapter = bean.getClass().getAnnotation(MessageAdapter.class);
            if (messageAdapter == null) {
                logger.warn("实例：{}，消息处理器：{}，未使用注解MessageAdapter，将不会被加载", instanceId, beanName);
                return;
            }
            if (messageAdapter.isBroadcast() && messageAdapter.isBatch()) {
                logger.warn("实例：{}，消息处理器：{}，同时声明了广播和批量消息，将不会被加载", instanceId, beanName);
                return;
            }

            if (messageAdapter.instanceId().equals(instanceId) && predicate.test(messageAdapter)) {
                String topicName = ConvertUtil.convertName(messageAdapter.topicName(), config);
                messageHandlers.put(topicName, bean);
            }
        });
        return messageHandlers;
    }

    private void startConsumers(String instanceId, MqsProperties.AdapterProperties config, Map<String, MessageHandler> messageHandlers, Map<String, MessageHandler> batchMessageHandlers, Map<String, MessageHandler> broadcastHandlers) {
        if (config.getVendorName().equals("rocketmq")) {
            validateRocketmqConfig(instanceId, config);
            if (!messageHandlers.isEmpty())
                consumerManager.add(new RocketmqTranConsumerFactory().startConsumer(instanceId, config, messageHandlers));
            if (!batchMessageHandlers.isEmpty())
                consumerManager.add(new RocketmqBatchConsumerFactory().startConsumer(instanceId, config, batchMessageHandlers));
            if (!broadcastHandlers.isEmpty())
                consumerManager.add(new RocketmqBroadcastConsumerFactory().startConsumer(instanceId, config, broadcastHandlers));
        } else if (config.getVendorName().equals("kafka")) {
            validateKafkaConfig(instanceId, config);
            if (!messageHandlers.isEmpty())
                consumerManager.add(new KafkaTranConsumerFactory().startConsumer(instanceId, config, messageHandlers));
            if (!batchMessageHandlers.isEmpty())
                consumerManager.add(new KafkaBatchConsumerFactory().startConsumer(instanceId, config, batchMessageHandlers));
            if (!broadcastHandlers.isEmpty())
                consumerManager.add(new KafkaBroadcastConsumerFactory().startConsumer(instanceId, config, broadcastHandlers));
        } else {
            logger.warn("厂商类型{}暂未支持", config.getVendorName());
        }
    }

    private void validateRocketmqConfig(String instanceId, MqsProperties.AdapterProperties config) {
        if (StringUtils.isBlank(config.getRocketmq().getClientConfig().getNamesrvAddr())) {
            logger.error("实例：{}，消费者启动失败，地址不能为空", instanceId);
            throw new IllegalArgumentException("namesrvAddr 不能为空");
        }
    }

    private void validateKafkaConfig(String instanceId, MqsProperties.AdapterProperties config) {
        if (StringUtils.isBlank(config.getKafka().getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
            logger.error("实例：{}，消费者启动失败，bootstrap.servers不能为空", instanceId);
            throw new IllegalArgumentException("bootstrap.servers 不能为空");
        }
    }

}
