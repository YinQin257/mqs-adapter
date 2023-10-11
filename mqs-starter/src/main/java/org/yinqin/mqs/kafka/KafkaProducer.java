package org.yinqin.mqs.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.yinqin.mqs.common.Consts;
import org.yinqin.mqs.common.config.MqsProperties.KafkaProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.entity.MessageCallback;
import org.yinqin.mqs.common.entity.MessageSendResult;
import org.yinqin.mqs.common.service.MessageProducer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * &#064;description:
 * &#064;author: YinQin
 * &#064;date: 2023-10-10 14:48
 */
public class KafkaProducer implements MessageProducer {

    private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer;

    private final KafkaProperties kafkaProperties;

    public KafkaProducer(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void start() throws Exception {
        Properties properties = kafkaProperties.getClientConfig();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
    }

    @Override
    public MessageSendResult sendMessage(AdapterMessage message) {
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(message.getTopic(), null, message.getBizKey(), message.getBody(), null);
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

    @Override
    public MessageSendResult sendMessage(AdapterMessage message, long timeout, TimeUnit unit) {
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(message.getTopic(), null, message.getBizKey(), message.getBody(), null);
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

    @Override
    public void sendMessage(AdapterMessage message, MessageCallback callback) {
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(message.getTopic(), null, message.getBizKey(), message.getBody(), null);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {//成功发送
                if (callback != null) callback.onSuccess();
            } else {
                //发送失败
                if (callback != null) callback.onError(e);
            }
        });
    }

    @Override
    public void destroy() throws Exception {
        kafkaProducer.close();
    }
}
