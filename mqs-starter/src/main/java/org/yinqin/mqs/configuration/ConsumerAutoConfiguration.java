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
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.manager.ConsumerManager;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.kafka.KafkaConsumer;
import org.yinqin.mqs.rocketmq.RocketmqConsumer;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;


@Configuration
public class ConsumerAutoConfiguration implements InitializingBean, DisposableBean, ApplicationContextAware, PriorityOrdered {

    private final Logger logger = LoggerFactory.getLogger(ConsumerAutoConfiguration.class);

    private ApplicationContext applicationContext;

    private final ConsumerManager consumerManager = new ConsumerManager();

    @Resource
    MqsProperties properties;

    @Override
    public void destroy() throws Exception {
        consumerManager.forEach((key, value) -> {
            try {
                value.destroy();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Map<String, MessageHandler> messageHandlerBeans = applicationContext.getBeansOfType(MessageHandler.class);
        properties.getRocketmq().forEach((vendorName, item) -> {
            if (StringUtils.isBlank(item.getGroupName())) {
                logger.error("消费者{}启动失败,groupName不能为空",vendorName);
                return;
            }
            if (StringUtils.isBlank(item.getClientConfig().getNamesrvAddr())) {
                logger.error("消费者{}启动失败，namesrvAddr不能为空",vendorName);
                return;
            }

            Map<String, MessageHandler> messageHandlers = new HashMap<>();
            Map<String, MessageHandler> transactionHandlers = new HashMap<>();
            Map<String, MessageHandler> broadcastHandlers = new HashMap<>();
            messageHandlerBeans.forEach((beanName, bean) -> {
                MessageAdapter messageAdapter = bean.getClass().getAnnotation(MessageAdapter.class);
                if (messageAdapter != null && messageAdapter.vendorName().equals(vendorName)) {
                    if (messageAdapter.isTransaction()) {
                        transactionHandlers.put(messageAdapter.topicName(), bean);
                    } else if (messageAdapter.isBroadcast()) {
                        broadcastHandlers.put(messageAdapter.topicName(), bean);
                    } else {
                        messageHandlers.put(messageAdapter.topicName(), bean);
                    }
                }
            });
            try {
                MessageConsumer consumer = new RocketmqConsumer(item,messageHandlers, transactionHandlers, broadcastHandlers);
                consumer.start();
                consumerManager.put(vendorName, consumer);
            } catch (Exception e) {
                logger.error("消费者{}启动失败", vendorName, e);
            }
        });

        properties.getKafka().forEach((vendorName, item) -> {
            if (StringUtils.isBlank(item.getGroupName())) {
                logger.error("消费者{}启动失败,groupName不能为空",vendorName);
                return;
            }
            if (StringUtils.isBlank(item.getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
                logger.error("消费者{}启动失败，bootstrap.servers不能为空",vendorName);
                return;
            }

            Map<String, MessageHandler> messageHandlers = new HashMap<>();
            Map<String, MessageHandler> transactionHandlers = new HashMap<>();
            Map<String, MessageHandler> broadcastHandlers = new HashMap<>();
            messageHandlerBeans.forEach((beanName, bean) -> {
                MessageAdapter messageAdapter = bean.getClass().getAnnotation(MessageAdapter.class);
                if (messageAdapter != null && messageAdapter.vendorName().equals(vendorName)) {
                    if (messageAdapter.isTransaction()) {
                        transactionHandlers.put(messageAdapter.topicName(), bean);
                    } else if (messageAdapter.isBroadcast()) {
                        broadcastHandlers.put(messageAdapter.topicName(), bean);
                    } else {
                        messageHandlers.put(messageAdapter.topicName(), bean);
                    }
                }
            });
            try {
                MessageConsumer consumer = new KafkaConsumer(item,messageHandlers, transactionHandlers, broadcastHandlers);
                consumer.start();
                consumerManager.put(vendorName, consumer);
            } catch (Exception e) {
                logger.error("消费者{}启动失败", vendorName, e);
            }
        });
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
