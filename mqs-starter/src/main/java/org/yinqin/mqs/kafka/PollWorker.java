package org.yinqin.mqs.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.entity.AdapterMessage;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.yinqin.mqs.common.handler.MessageHandler;

/**
 * @description 拉取消息工作线程
 * @author YinQin
 * @createTime 2023-10-10 16:04
 */
public class PollWorker implements Runnable{

    private final Logger logger = LoggerFactory.getLogger(PollWorker.class);

    /**
     * 线程停止标记
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 消费类型
     */
    private String consumerType;

    /**
     * kafka源生消费者
     */
    private KafkaConsumer<String, byte[]> kafkaConsumer;

    /**
     * 消息处理器集合
     */
    private Map<String, MessageHandler> messageHandlers;

    public PollWorker(String consumerType, KafkaConsumer<String, byte[]> kafkaConsumer, Map<String, MessageHandler> messageHandlers) {
        this.consumerType = consumerType;
        this.kafkaConsumer = kafkaConsumer;
        this.messageHandlers = messageHandlers;
    }

    public PollWorker() {
    }

    @Override
    public void run() {
        while (!closed.get()) {
            try {
                List<AdapterMessage> messages = fetchMessages();
                if (messages.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }
                if (consumerType.equals("BATCH")) {
                    batchConsumeMessage(messages);
                } else {
                    messages.forEach(this::consumeMessage);
                }
            } catch (Exception e) {
                logger.error("拉取消息异常：", e);
            }
        }
    }

    public void shutdown() {
        closed.set(true);
    }

    /**
     * 拉取消息
     * @return 消息集合
     */
    private List<AdapterMessage> fetchMessages() {
        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
        Iterator<ConsumerRecord<String, byte[]>> iterator = records.iterator();
        List<AdapterMessage> messages = new ArrayList<>(records.count());
        ConsumerRecord<String, byte[]> item;
        while(iterator.hasNext()) {
            item = iterator.next();
            AdapterMessage message = AdapterMessage.builder()
                    .topic(item.topic())
                    .body(item.value())
                    .bizKey(item.key())
                    .build();
            message.setOriginMessage(item);
            messages.add(message);
        }
        return messages;
    }

    /**
     * 按照topic分组批量消费消息
     * @param messages 消息集合
     */
    private void batchConsumeMessage(List<AdapterMessage> messages) {
        Map<String, List<AdapterMessage>> collect = messages.stream().collect(Collectors.groupingBy(AdapterMessage::getTopic, Collectors.toList()));
        try {
            for (Map.Entry<String, List<AdapterMessage>> entry : collect.entrySet()) {
                String topic = entry.getKey();
                List<AdapterMessage> messageObjectList = entry.getValue();
                logger.debug("kafka批量消息，topic：{},消息数量为：{}", topic, messageObjectList.size());
                messageHandlers.get(topic).process(messageObjectList);
            }
        } catch (Exception e) {
            logger.error("批量消费失败：", e);
        }
    }

    /**
     * 单条消费消息
     * @param message 消息
     */
    private void consumeMessage(AdapterMessage message) {
        MessageHandler messageHandler = messageHandlers.get(message.getTopic());
        try {
            messageHandler.process(message);
        } catch (Exception e) {
            logger.error("单条消费失败：", e);
        }
    }
}
