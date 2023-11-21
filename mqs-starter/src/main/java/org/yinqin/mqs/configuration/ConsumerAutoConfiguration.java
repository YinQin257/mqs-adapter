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
import org.yinqin.mqs.kafka.KafkaConsumer;
import org.yinqin.mqs.rocketmq.RocketmqConsumer;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * 消息适配器消费者自动装配类
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年10月13日
 * @see org.springframework.beans.factory.InitializingBean
 * @see org.springframework.beans.factory.DisposableBean
 * @see org.springframework.context.ApplicationContextAware
 * @see org.springframework.core.PriorityOrdered
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
     *
     */
    @Override
    public void destroy() {
        for (Map.Entry<String, MessageConsumer> entry : consumerManager.entrySet()) {
            String consumerType = entry.getKey();
            MessageConsumer consumer = entry.getValue();
            try {
                consumer.destroy();
            } catch (Exception e) {
                logger.error("注销{}消费组失败：", consumerType, e);
            }
        }
    }

    /**
     * 实现InitializingBean接口
     * 启动所有的消费组
     *
     */
    @Override
    public void afterPropertiesSet() {
        Map<String, MessageHandler> messageHandlerBeans = applicationContext.getBeansOfType(MessageHandler.class);
        properties.getAdapter().forEach((instanceId, config) -> {
            if (!config.isConsumerEnabled()) return;
            if (StringUtils.isBlank(config.getVendorName())) {
                logger.error("生产者{}启动失败,vendorNam不能为空", instanceId);
                return;
            }
            if (StringUtils.isBlank(config.getGroupName())) {
                logger.error("消费者{}启动失败,groupName不能为空", instanceId);
                return;
            }
            String vendorName = config.getVendorName();
            Map<String, MessageHandler> messageHandlers = new HashMap<>();
            Map<String, MessageHandler> batchMessageHandlers = new HashMap<>();
            Map<String, MessageHandler> broadcastHandlers = new HashMap<>();
            messageHandlerBeans.forEach((beanName, bean) -> {
                MessageAdapter messageAdapter = bean.getClass().getAnnotation(MessageAdapter.class);
                if (messageAdapter != null && messageAdapter.instanceId().equals(instanceId)) {
                    String topicName = ConvertUtil.convertName(messageAdapter.topicName(), config.getTopic());
                    if (messageAdapter.isBroadcast()) {
                        broadcastHandlers.put(topicName, bean);
                    } else if (messageAdapter.isBatch()) {
                        batchMessageHandlers.put(topicName, bean);
                    } else {
                        messageHandlers.put(topicName, bean);
                    }
                }
            });
            MessageConsumer consumer = null;
            if (config.getVendorName().equals("rocketmq")) {
                if (StringUtils.isBlank(config.getRocketmq().getClientConfig().getNamesrvAddr())) {
                    logger.error("消费者{}启动失败，namesrvAddr不能为空", vendorName);
                    return;
                }
                consumer = new RocketmqConsumer(config, batchMessageHandlers, messageHandlers, broadcastHandlers);
            } else if (config.getVendorName().equals("kafka")) {
                if (StringUtils.isBlank(config.getKafka().getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
                    logger.error("消费者{}启动失败，bootstrap.servers不能为空", vendorName);
                    return;
                }
                consumer = new KafkaConsumer(config, batchMessageHandlers, messageHandlers, broadcastHandlers);
            } else {
                logger.warn("厂商类型{}暂未支持", config.getVendorName());
            }
            if (consumer == null) return;
            try {
                consumer.start();
                consumerManager.put(vendorName, consumer);
                logger.info("消费者{}启动成功", vendorName);
            } catch (Exception e) {
                logger.error("消费者{}启动失败", vendorName, e);
            }

        });
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

}
