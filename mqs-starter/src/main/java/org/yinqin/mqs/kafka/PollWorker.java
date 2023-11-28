package org.yinqin.mqs.kafka;

import cn.hutool.core.thread.ThreadUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 拉取消息工作线程
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年10月13日
 * @see Runnable
 * @since 1.0.0
 */
public class PollWorker implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(PollWorker.class);

    /**
     * 线程停止标记
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * kafka源生消费者
     */
    private final KafkaConsumer<String, byte[]> kafkaConsumer;

    /**
     * 消息处理器集合
     */
    private final Map<String, MessageHandler> messageHandlers;

    /**
     * 拉取消息间隔
     */
    private final int interval;

    public PollWorker(KafkaConsumer<String, byte[]> kafkaConsumer, Map<String, MessageHandler> messageHandlers, int interval) {
        this.kafkaConsumer = kafkaConsumer;
        this.messageHandlers = messageHandlers;
        this.interval = interval;
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                try {
                    List<AdapterMessage> messages = fetchMessages();
                    if (messages.isEmpty()) {
                        ThreadUtil.sleep(interval);
                        continue;
                    }

                    consumeMessage(messages);
                } catch (Exception e) {
                    logger.error("拉取消息异常：", e);
                }
            }
        } finally {
            // 关闭消费者,必须在当前线程关闭，否则会有线程安全问题
            kafkaConsumer.close();
        }

    }

    public void shutdown() {
        closed.set(true);
    }

    /**
     * 拉取消息
     *
     * @return 消息集合
     */
    private List<AdapterMessage> fetchMessages() {
        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
        Iterator<ConsumerRecord<String, byte[]>> iterator = records.iterator();
        List<AdapterMessage> messages = new ArrayList<>(records.count());
        ConsumerRecord<String, byte[]> item;
        while (iterator.hasNext()) {
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
     *
     * @param messages 消息集合
     */
    private void consumeMessage(List<AdapterMessage> messages) {
        Map<String, List<AdapterMessage>> collect = messages.stream().collect(Collectors.groupingBy(AdapterMessage::getTopic, Collectors.toList()));
        try {
            for (Map.Entry<String, List<AdapterMessage>> entry : collect.entrySet()) {
                String topic = entry.getKey();
                List<AdapterMessage> messageObjectList = entry.getValue();
                logger.debug("kafka批量消息，topic：{},消息数量为：{}", topic, messageObjectList.size());
                MessageAdapter messageAdapter = messageHandlers.get(messageObjectList.get(0).getTopic()).getClass().getAnnotation(MessageAdapter.class);
                if (messageAdapter.isBatch()) {
                    messageHandlers.get(messageObjectList.get(0).getTopic()).process(messages);
                } else {
                    for (AdapterMessage msg : messages)
                        messageHandlers.get(messageObjectList.get(0).getTopic()).process(msg);
                }
            }
        } catch (Exception e) {
            logger.error("kafka消费异常：", e);
        }
    }
}
