package org.yinqin.mqs.rocketmq.producer;

import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.factory.ProducerFactory;
import org.yinqin.mqs.common.service.MessageProducer;

/**
 * Rocketmq生产者工厂类
 * @author YinQin
 * @createDate 2023年11月27日
 * @since 1.0.6
 * @version 1.0.6
 */
public class RocketmqProducerFactory implements ProducerFactory {
    @Override
    public MessageProducer createProducer(String instanceId, MqsProperties.AdapterProperties rocketmqProperties) {
        CustomRocketmqProducer customRocketmqProducer = new CustomRocketmqProducer(instanceId, rocketmqProperties);
        customRocketmqProducer.start();
        return customRocketmqProducer;
    }
}
