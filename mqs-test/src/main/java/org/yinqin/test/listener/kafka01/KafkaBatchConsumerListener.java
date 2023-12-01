package org.yinqin.test.listener.kafka01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.manager.ProducerManager;
import org.yinqin.test.listener.rocketmq02.SequentialConsumer;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * kafka批量集群消费监听器
 *
 * @author YinQin
 * @version 1.0.5
 * @createDate 2023年11月20日
 * @since 1.0.3
 */
@Component("kafkaBatchConsumerListener-default")
@MessageAdapter(topicName = "MQS_TEST_TOPIC_BATCH", isBatch = true)
public class KafkaBatchConsumerListener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBatchConsumerListener.class);

    @Autowired
    ProducerManager producerManager;

    @Override
    public void process(AdapterMessage message) throws Exception {

    }

    @Override
    public void process(List<AdapterMessage> messages) throws Exception {
        logger.info("监听到批量消息，消息总数为：{}", messages.size());
        messages.forEach(message -> {
            String messageStr = new String(message.getBody(), StandardCharsets.UTF_8);
            int i = Integer.parseInt(messageStr);
            if (SequentialConsumer.getInstance().getConsumerId().get() == i) {
                logger.info("顺序消费，TOPIC：{}，消息内容是：{}", message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
                SequentialConsumer.getInstance().getConsumerId().addAndGet(1);
            } else {
                producerManager.getDefaultInstance().sendMessage(message);
                logger.error("非顺序消费，拒绝消费");
            }
        });
    }
}
