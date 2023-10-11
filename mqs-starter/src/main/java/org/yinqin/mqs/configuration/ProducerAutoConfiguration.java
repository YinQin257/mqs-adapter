package org.yinqin.mqs.configuration;


import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.service.MessageProducer;
import org.yinqin.mqs.common.manager.ProducerManager;
import org.yinqin.mqs.kafka.KafkaProducer;
import org.yinqin.mqs.rocketmq.RocketmqProducer;

/**
 * @description 消息适配器生产者自动装配类，将生产者管理器注入IOC容器，通过自定义组件名称获取对应的生产者
 * @author YinQin
 * @createTime 2023-09-28 11:44
 */
@Configuration
@EnableConfigurationProperties({ MqsProperties.class })
public class ProducerAutoConfiguration {

    private final Logger logger = LoggerFactory.getLogger(ProducerAutoConfiguration.class);

    @Bean
    public ProducerManager getProducerManager (MqsProperties properties) {
        ProducerManager producerManager = new ProducerManager();
        properties.getRocketmq().forEach((vendorName, item) -> {

            if (!item.isProducerEnabled()) return;

            // check properties
            if (StringUtils.isBlank(item.getGroupName())) {
                logger.error("生产者{}启动失败,groupName不能为空",vendorName);
                return;
            }
            if (StringUtils.isBlank(item.getClientConfig().getNamesrvAddr())) {
                logger.error("生产者{}启动失败，namesrvAddr不能为空",vendorName);
                return;
            }

            MessageProducer rocketmqProducer = new RocketmqProducer(item);
            try {
                rocketmqProducer.start();
                producerManager.put(vendorName, rocketmqProducer);
                logger.info("生产者{}启动成功",vendorName);
            } catch (Exception e) {
                logger.error("生产者{}启动失败",vendorName,e);
            }
        });
        properties.getKafka().forEach((vendorName, item) -> {

            if (!item.isProducerEnabled()) return;
            // check properties
            if (StringUtils.isBlank(item.getGroupName())) {
                logger.error("生产者{}启动失败,groupName不能为空",vendorName);
                return;
            }
            if (StringUtils.isBlank(item.getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
                logger.error("生产者{}启动失败，bootstrap.servers不能为空",vendorName);
                return;
            }

            MessageProducer rocketmqProducer = new KafkaProducer(item);
            try {
                rocketmqProducer.start();
                producerManager.put(vendorName, rocketmqProducer);
                logger.info("生产者{}启动成功",vendorName);
            } catch (Exception e) {
                logger.error("生产者{}启动失败",vendorName,e);
            }
        });
        return producerManager;
    }

}
