package org.yinqin.mqs.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.entity.AdapterMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.yinqin.mqs.common.handler.MessageHandler;

/**
 * &#064;description:
 * &#064;author: YinQin
 * &#064;date: 2023-10-10 16:04
 */
public class PollWorker implements Runnable{

    private final Logger logger = LoggerFactory.getLogger(PollWorker.class);

    private AtomicBoolean closed = new AtomicBoolean(false);

    private final String consumerType;

    private final KafkaConsumer<String, byte[]> kafkaConsumer;

    private final Map<String, MessageHandler> messageHandlers;

    public PollWorker(String consumerType, KafkaConsumer<String, byte[]> kafkaConsumer, Map<String, MessageHandler> messageHandlers) {
        this.consumerType = consumerType;
        this.kafkaConsumer = kafkaConsumer;
        this.messageHandlers = messageHandlers;
    }

    @Override
    public void run() {
        while (!closed.get()) {
            try {
                List<AdapterMessage> messages = fetchMessages();
                if (messages == null || messages.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }
                if (consumerType.equals("BATCH")) {
                    batchConsumeMessage(messages);
                } else {
                    messages.forEach(this::consumeMessage);
                }


            } catch (Exception e) {

            }
        }
    }

    public void shutdown() {
        closed.set(true);
    }

    private List<AdapterMessage> fetchMessages() {
        //手动提交offset
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
        }
    }

    private void consumeMessage(AdapterMessage message) {
        MessageHandler messageHandler = messageHandlers.get(message.getTopic());
        try {
            messageHandler.process(message);
        } catch (Exception e) {

        }
    }
}
