package org.yinqin.mqs.configuration;


import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.factory.ProducerFactory;
import org.yinqin.mqs.common.manager.ProducerManager;
import org.yinqin.mqs.kafka.producer.KafkaProducerFactory;
import org.yinqin.mqs.rocketmq.producer.RocketmqProducerFactory;

/**
 * 消息适配器生产者自动装配类，将生产者管理器注入IOC容器，通过自定义组件名称获取对应的生产者
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.config.MqsProperties
 * @since 1.0.0
 */
@Configuration
@EnableConfigurationProperties({MqsProperties.class})
public abstract class ProducerAutoConfiguration {

    private final Logger logger = LoggerFactory.getLogger(ProducerAutoConfiguration.class);

    @Bean
    public ProducerManager getProducerManager(MqsProperties properties) {
        ProducerManager producerManager = new ProducerManager();
        ProducerFactory rocketmqProducerFactory = new RocketmqProducerFactory();
        ProducerFactory kafkaProducerFactory = new KafkaProducerFactory();

        properties.getAdapter().forEach((instanceId, config) -> {
            if (!config.isProducerEnabled()) return;
            if (StringUtils.isBlank(config.getVendorName())) {
                logger.error("实例：{}，生产者启动失败,vendorNam不能为空", instanceId);
                return;
            }
            if (StringUtils.isBlank(config.getGroupName())) {
                logger.error("实例：{}，生产者启动失败,groupName不能为空", instanceId);
                return;
            }
            if (config.getVendorName().equals("rocketmq")) {
                if (StringUtils.isBlank(config.getRocketmq().getClientConfig().getNamesrvAddr())) {
                    logger.error("实例：{}，生产者启动失败，地址不能为空", instanceId);
                    return;
                }
                producerManager.put(instanceId, rocketmqProducerFactory.createProducer(instanceId, config));
            } else if (config.getVendorName().equals("kafka")) {
                if (StringUtils.isBlank(config.getKafka().getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
                    logger.error("实例：{}，生产者启动失败，bootstrap.servers不能为空", instanceId);
                    return;
                }
                producerManager.put(instanceId, kafkaProducerFactory.createProducer(instanceId, config));
            } else {
                logger.warn("厂商类型{}暂未支持", config.getVendorName());
            }
        });
        return producerManager;
    }

}
