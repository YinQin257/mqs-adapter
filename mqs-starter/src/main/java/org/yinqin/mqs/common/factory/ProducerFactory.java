package org.yinqin.mqs.common.factory;

import org.yinqin.mqs.common.config.MqsProperties;
import org.yinqin.mqs.common.service.MessageProducer;

/**
 * 生产者工厂抽象接口
 *
 * @author YinQin
 * @createDate 2023年11月27日
 * @since 1.0.6
 * @version 1.0.6
 */
public abstract class ProducerFactory {

    /**
     * 工厂方法
     * @param instanceId 实例ID
     * @param properties 配置
     * @return 生产者
     */
    public abstract MessageProducer createProducer(String instanceId, MqsProperties.AdapterProperties properties);

    /**
     * 启动生产者
     * @param instanceId 实例ID
     * @param properties 配置类
     * @return 生产者实例
     */
    public MessageProducer startProducer(String instanceId, MqsProperties.AdapterProperties properties){
        MessageProducer producer = createProducer(instanceId, properties);
        producer.start();
        return producer;
    }
}
