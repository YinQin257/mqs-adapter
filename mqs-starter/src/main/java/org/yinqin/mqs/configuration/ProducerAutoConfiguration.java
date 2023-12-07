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
 * @see MqsProperties
 * @since 1.0.0
 */
@Configuration
@EnableConfigurationProperties({MqsProperties.class})
public class ProducerAutoConfiguration {

    // 日志记录器
    private final Logger logger = LoggerFactory.getLogger(ProducerAutoConfiguration.class);

    /**
     * 创建并初始化生产者管理器。
     *
     * @param properties 配置属性
     * @return 生产者管理器
     */
    @Bean
    public ProducerManager getProducerManager(MqsProperties properties) {
        // 创建生产者管理器实例
        ProducerManager producerManager = new ProducerManager();

        // 创建RocketMQ生产者工厂实例
        ProducerFactory rocketmqProducerFactory = new RocketmqProducerFactory();

        // 创建Kafka生产者工厂实例
        ProducerFactory kafkaProducerFactory = new KafkaProducerFactory();

        // 遍历配置中的所有适配器实例
        properties.getAdapter().forEach((instanceId, config) -> {
            // 如果生产者未启用，则跳过
            if (!config.isProducerEnabled()) return;

            // 验证配置项是否有效
            validateConfig(instanceId, config);

            // 根据厂商类型启动生产者
            startProducer(instanceId, config, producerManager, rocketmqProducerFactory, kafkaProducerFactory);
        });

        // 返回初始化完成的生产者管理器
        return producerManager;
    }

    /**
     * 验证配置项是否有效。
     *
     * @param instanceId 实例ID
     * @param config 配置项
     */
    private void validateConfig(String instanceId, MqsProperties.AdapterProperties config) {
        // 验证vendorName是否为空
        if (StringUtils.isBlank(config.getVendorName())) {
            logger.error("实例：{}，生产者启动失败,vendorName不能为空", instanceId);
            throw new IllegalArgumentException("vendorName 不能为空");
        }

        // 验证groupName是否为空
        if (StringUtils.isBlank(config.getGroupName())) {
            logger.error("实例：{}，生产者启动失败,groupName不能为空", instanceId);
            throw new IllegalArgumentException("groupName 不能为空");
        }
    }

    /**
     * 根据厂商类型启动生产者。
     *
     * @param instanceId 实例ID
     * @param config 配置项
     * @param producerManager 生产者管理器
     * @param rocketmqProducerFactory RocketMQ生产者工厂
     * @param kafkaProducerFactory Kafka生产者工厂
     */
    private void startProducer(String instanceId, MqsProperties.AdapterProperties config, ProducerManager producerManager, ProducerFactory rocketmqProducerFactory, ProducerFactory kafkaProducerFactory) {
        // 获取厂商类型
        String vendorName = config.getVendorName();

        // 如果厂商类型是RocketMQ
        if (vendorName.equals("rocketmq")) {
            // 验证RocketMQ配置项是否有效
            validateRocketmqConfig(instanceId, config);

            // 启动RocketMQ生产者并添加到生产者管理器
            producerManager.put(instanceId, rocketmqProducerFactory.startProducer(instanceId, config));
        }
        // 如果厂商类型是Kafka
        else if (vendorName.equals("kafka")) {
            // 验证Kafka配置项是否有效
            validateKafkaConfig(instanceId, config);

            // 启动Kafka生产者并添加到生产者管理器
            producerManager.put(instanceId, kafkaProducerFactory.startProducer(instanceId, config));
        }
        // 如果厂商类型不支持
        else {
            logger.warn("厂商类型{}暂未支持", vendorName);
        }
    }

    /**
     * 验证RocketMQ配置项是否有效。
     *
     * @param instanceId 实例ID
     * @param config 配置项
     */
    private void validateRocketmqConfig(String instanceId, MqsProperties.AdapterProperties config) {
        // 验证namesrvAddr是否为空
        if (StringUtils.isBlank(config.getRocketmq().getClientConfig().getNamesrvAddr())) {
            logger.error("实例：{}，生产者启动失败，地址不能为空", instanceId);
            throw new IllegalArgumentException("namesrvAddr 不能为空");
        }
    }

    /**
     * 验证Kafka配置项是否有效。
     *
     * @param instanceId 实例ID
     * @param config 配置项
     */
    private void validateKafkaConfig(String instanceId, MqsProperties.AdapterProperties config) {
        // 验证bootstrap.servers是否为空
        if (StringUtils.isBlank(config.getKafka().getClientConfig().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
            logger.error("实例：{}，生产者启动失败，bootstrap.servers不能为空", instanceId);
            throw new IllegalArgumentException("bootstrap.servers 不能为空");
        }
    }

}
