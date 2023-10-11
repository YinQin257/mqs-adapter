package org.yinqin.mqs.configuration;


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
import org.yinqin.mqs.common.config.MqsProperties.*;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.manager.ConsumerManager;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.kafka.KafkaConsumer;
import org.yinqin.mqs.rocketmq.RocketmqConsumer;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * @description 消息适配器消费者自动装配类，在spring容器初始化成功后启动所有消费者
 * @author YinQin
 * @createTime 2023-09-28 11:44
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
     * @throws Exception none
     */
    @Override
    public void destroy() throws Exception {
        consumerManager.forEach((consumerType, consumer) -> {
            try {
                consumer.destroy();
            } catch (Exception e) {
                logger.error("注销{}消费组失败：",consumerType,e);
            }
        });
    }

    /**
     * 实现InitializingBean接口
     * 启动所有的消费组
     * @throws Exception none
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Map<String, MessageHandler> messageHandlerBeans = applicationContext.getBeansOfType(MessageHandler.class);
        properties.getRocketmq().forEach((vendorName, rocketmqProperties) -> {
            if (!rocketmqProperties.isProducerEnabled()) return;
            if (StringUtils.isBlank(rocketmqProperties.getGroupName())) {
                logger.error("消费者{}启动失败,groupName不能为空",vendorName);
                return;
            }
            if (StringUtils.isBlank(rocketmqProperties.getClientConfig().getNamesrvAddr())) {
                logger.error("消费者{}启动失败，namesrvAddr不能为空",vendorName);
                return;
            }
            rocketmqConsumerStart(messageHandlerBeans,vendorName,rocketmqProperties);

        });

        properties.getKafka().forEach((vendorName, kafkaProperties) -> {
            if (!kafkaProperties.isProducerEnabled()) return;
            if (StringUtils.isBlank(kafkaProperties.getGroupName())) {
                logger.error("消费者{}启动失败,groupName不能为空",vendorName);
                return;
            }
            if (StringUtils.isBlank(kafkaProperties.getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
                logger.error("消费者{}启动失败，bootstrap.servers不能为空",vendorName);
                return;
            }
            kafkaConsumerStart(messageHandlerBeans,vendorName,kafkaProperties);

        });
    }

    /**
     * rocketmq消费组启动方法
     * @param messageHandlerBeans 所有实现MessageHandler接口的bean
     * @param vendorName 自定义组件名称
     * @param rocketmqProperties rocketmq配置
     */
    private void rocketmqConsumerStart(Map<String, MessageHandler> messageHandlerBeans, String vendorName, RocketmqProperties rocketmqProperties) {
        Map<String, MessageHandler> messageHandlers = new HashMap<>();
        Map<String, MessageHandler> batchMessageHandlers = new HashMap<>();
        Map<String, MessageHandler> broadcastHandlers = new HashMap<>();
        messageHandlerBeans.forEach((beanName, bean) -> {
            MessageAdapter messageAdapter = bean.getClass().getAnnotation(MessageAdapter.class);
            if (messageAdapter != null && messageAdapter.vendorName().equals(vendorName)) {
                if (messageAdapter.isBatch()) {
                    batchMessageHandlers.put(messageAdapter.topicName(), bean);
                } else if (messageAdapter.isBroadcast()) {
                    broadcastHandlers.put(messageAdapter.topicName(), bean);
                } else {
                    messageHandlers.put(messageAdapter.topicName(), bean);

                }
            }
        });
        try {
            MessageConsumer consumer = new RocketmqConsumer(rocketmqProperties,batchMessageHandlers, messageHandlers, broadcastHandlers);
            consumer.start();
            consumerManager.put(vendorName, consumer);
        } catch (Exception e) {
            logger.error("消费者{}启动失败", vendorName, e);
        }
    }

    /**
     * kafka消费组启动方法
     * @param messageHandlerBeans 所有实现MessageHandler接口的bean
     * @param vendorName 自定义组件名称
     * @param kafkaProperties kafka配置
     */
    private void kafkaConsumerStart(Map<String, MessageHandler> messageHandlerBeans, String vendorName, KafkaProperties kafkaProperties) {
        Map<String, MessageHandler> messageHandlers = new HashMap<>();
        Map<String, MessageHandler> batchMessageHandlers = new HashMap<>();
        Map<String, MessageHandler> broadcastHandlers = new HashMap<>();
        messageHandlerBeans.forEach((beanName, bean) -> {
            MessageAdapter messageAdapter = bean.getClass().getAnnotation(MessageAdapter.class);
            if (messageAdapter != null && messageAdapter.vendorName().equals(vendorName)) {
                if (messageAdapter.isBatch()) {
                    batchMessageHandlers.put(messageAdapter.topicName(), bean);
                } else if (messageAdapter.isBroadcast()) {
                    broadcastHandlers.put(messageAdapter.topicName(), bean);
                } else {
                    messageHandlers.put(messageAdapter.topicName(), bean);
                }
            }
        });
        try {
            MessageConsumer consumer = new KafkaConsumer(kafkaProperties, batchMessageHandlers, messageHandlers, broadcastHandlers);
            consumer.start();
            consumerManager.put(vendorName, consumer);
        } catch (Exception e) {
            logger.error("消费者{}启动失败", vendorName, e);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

}
