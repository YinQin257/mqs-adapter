package org.yinqin.mqs.configuration;


import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.manager.ProducerManager;
import org.yinqin.mqs.common.service.MessageProducer;
import org.yinqin.mqs.kafka.KafkaProducer;
import org.yinqin.mqs.rocketmq.RocketmqProducer;

/**
 * 消息适配器生产者自动装配类，将生产者管理器注入IOC容器，通过自定义组件名称获取对应的生产者
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.config.MqsProperties
 * @since 1.0.0
 */
@Configuration
@EnableConfigurationProperties({MqsProperties.class})
public class ProducerAutoConfiguration {

    private final Logger logger = LoggerFactory.getLogger(ProducerAutoConfiguration.class);

    @Bean
    public ProducerManager getProducerManager(MqsProperties properties) {
        ProducerManager producerManager = new ProducerManager();

        properties.getAdapter().forEach((instanceId, config) -> {
            if (!config.isProducerEnabled()) return;
            if (StringUtils.isBlank(config.getVendorName())) {
                logger.error("生产者{}启动失败,vendorNam不能为空", instanceId);
                return;
            }
            if (StringUtils.isBlank(config.getGroupName())) {
                logger.error("生产者{}启动失败,groupName不能为空", instanceId);
                return;
            }
            String vendorName = config.getVendorName();
            MessageProducer producer = null;
            if (config.getVendorName().equals("rocketmq")) {
                if (StringUtils.isBlank(config.getRocketmq().getClientConfig().getNamesrvAddr())) {
                    logger.error("生产者{}启动失败，namesrvAddr不能为空", instanceId);
                    return;
                }
                producer = new RocketmqProducer(config);
                try {
                    producer.start();
                } catch (Exception e) {
                    logger.error("生产者{}启动失败", instanceId, e);
                }

            } else if (config.getVendorName().equals("kafka")) {
                if (StringUtils.isBlank(config.getKafka().getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
                    logger.error("生产者{}启动失败，bootstrap.servers不能为空", instanceId);
                    return;
                }
                producer = new KafkaProducer(config);
                try {
                    producer.start();
                } catch (Exception e) {
                    logger.error("生产者{}启动失败", instanceId, e);
                }
            } else {
                logger.warn("厂商类型{}暂未支持", config.getVendorName());
                return;
            }
            logger.info("生产者{}启动成功", instanceId);
            producerManager.put(instanceId, producer);
        });
        return producerManager;
    }

}
