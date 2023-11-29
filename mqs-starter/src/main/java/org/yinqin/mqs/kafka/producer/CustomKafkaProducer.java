package org.yinqin.mqs.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.config.MqsProperties.AdapterProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;
import org.yinqin.mqs.common.service.MessageProducer;
import org.yinqin.mqs.common.util.ConvertUtil;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 自定义kafka生产者
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.service.MessageProducer
 * @since 1.0.0
 */
public class CustomKafkaProducer implements MessageProducer {

    private final Logger logger = LoggerFactory.getLogger(CustomKafkaProducer.class);

    /**
     * 实例ID
     */
    private final String instanceId;

    /**
     * 源生kafka消费者合集
     */
    private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer;

    /**
     * kafka配置类
     */
    private final AdapterProperties kafkaProperties;

    public CustomKafkaProducer(String instanceId, AdapterProperties kafkaProperties) {
        this.instanceId = instanceId;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * 启动生产者
     */
    @Override
    public void start() {
        logger.info("实例：{} 生产者启动中，启动配置：{}", instanceId, kafkaProperties.toString());
        Properties properties = kafkaProperties.getKafka().getClientConfig();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        logger.info("实例：{} 生产者启动中成功", instanceId);
    }

    /**
     * 同步发送消息方法
     *
     * @param message 消息
     * @return 消息处理结果
     */
    @Override
    public MessageSendResult sendMessage(AdapterMessage message) {
        ProducerRecord<String, byte[]> producerRecord = ConvertUtil.AdapterMessageToKafkaMessage(message, kafkaProperties.getTopic());
        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            future.get(3000, TimeUnit.MILLISECONDS);
            messageSendResult.setStatus(Constants.SUCCESS);
        } catch (Exception e) {
            messageSendResult.setStatus(Constants.ERROR);
            messageSendResult.setThrowable(e);
            logger.error("同步消息发送失败，失败原因：", e);
        }
        return messageSendResult;
    }

    /**
     * 同步发送消息方法
     *
     * @param message 消息
     * @param timeout 同步等待时间
     * @param unit    时间单位
     * @return 消息处理结果
     */
    @Override
    public MessageSendResult sendMessage(AdapterMessage message, long timeout, TimeUnit unit) {
        ProducerRecord<String, byte[]> producerRecord = ConvertUtil.AdapterMessageToKafkaMessage(message, kafkaProperties.getTopic());
        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            future.get(timeout, unit);
            messageSendResult.setStatus(Constants.SUCCESS);
        } catch (Exception e) {
            messageSendResult.setStatus(Constants.ERROR);
            messageSendResult.setThrowable(e);
            logger.error("同步消息发送失败，失败原因：", e);
        }
        return messageSendResult;
    }

    /**
     * 异步发送消息方法
     *
     * @param message  消息
     * @param callback 消息发送结果回调
     */
    @Override
    public void sendMessage(AdapterMessage message, MessageCallback callback) {
        ProducerRecord<String, byte[]> producerRecord = ConvertUtil.AdapterMessageToKafkaMessage(message, kafkaProperties.getTopic());
        try {
            kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                if (callback == null) return;
                if (e == null) callback.onSuccess();
                else callback.onError(e);
            });
        } catch (Exception e) {
            logger.error("异步消息发送失败，失败原因：", e);
        }
    }

    /**
     * 注销kafka生产者
     */
    @Override
    public void destroy() {
        kafkaProducer.close();
        logger.info("实例：{} 生产者停止成功", instanceId);
    }
}
