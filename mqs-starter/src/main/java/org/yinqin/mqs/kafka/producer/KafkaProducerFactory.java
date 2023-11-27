package org.yinqin.mqs.kafka.producer;

import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.factory.ProducerFactory;
import org.yinqin.mqs.common.service.MessageProducer;

/**
 * Kafka生产者工厂类
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年11月27日
 * @see org.yinqin.mqs.common.factory.ProducerFactory
 * @since 1.0.6
 */
public class KafkaProducerFactory implements ProducerFactory {
    @Override
    public MessageProducer createProducer(String instanceId, MqsProperties.AdapterProperties kafkaProperties) {
        CustomKafkaProducer customKafkaProducer = new CustomKafkaProducer(instanceId, kafkaProperties);
        customKafkaProducer.start();
        return customKafkaProducer;
    }
}
