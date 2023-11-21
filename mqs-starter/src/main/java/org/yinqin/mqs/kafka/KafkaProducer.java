package org.yinqin.mqs.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.yinqin.mqs.common.Consts;
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
 * kafka生产者
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.service.MessageProducer
 * @since 1.0.0
 */
public class KafkaProducer implements MessageProducer {

    /**
     * 源生kafka消费者合集
     */
    private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer;

    /**
     * kafka配置类
     */
    private final AdapterProperties kafkaProperties;

    public KafkaProducer(AdapterProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * 启动生产者
     *
     */
    @Override
    public void start() {
        Properties properties = kafkaProperties.getKafka().getClientConfig();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
    }

    /**
     * 同步发送消息方法
     *
     * @param message 消息
     * @return 消息处理结果
     */
    @Override
    public MessageSendResult sendMessage(AdapterMessage message) {
        ProducerRecord<String, byte[]> producerRecord = ConvertUtil.AdapterMessageToKafkaMessage(message,kafkaProperties.getTopic());
        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            future.get(3000, TimeUnit.MILLISECONDS);
            messageSendResult.setStatus(Consts.SUCCESS);
        } catch (Exception e) {
            messageSendResult.setStatus(Consts.ERROR);
            messageSendResult.setThrowable(e);
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
        ProducerRecord<String, byte[]> producerRecord = ConvertUtil.AdapterMessageToKafkaMessage(message,kafkaProperties.getTopic());
        MessageSendResult messageSendResult = new MessageSendResult();
        try {
            Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            future.get(timeout, unit);
            messageSendResult.setStatus(Consts.SUCCESS);
        } catch (Exception e) {
            messageSendResult.setStatus(Consts.ERROR);
            messageSendResult.setThrowable(e);
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
        ProducerRecord<String, byte[]> producerRecord = ConvertUtil.AdapterMessageToKafkaMessage(message,kafkaProperties.getTopic());
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {//成功发送
                if (callback != null) callback.onSuccess();
            } else {
                //发送失败
                if (callback != null) callback.onError(e);
            }
        });
    }

    /**
     * 注销kafka生产者
     *
     */
    @Override
    public void destroy() {
        kafkaProducer.close();
    }
}
