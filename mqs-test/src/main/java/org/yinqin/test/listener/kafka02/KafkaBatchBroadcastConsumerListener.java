package org.yinqin.test.listener.kafka02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * kafka批量广播消费监听器
 *
 * @author YinQin
 * @version 1.0.5
 * @createDate 2023年11月20日
 * @since 1.0.5
 */
@Component("KafkaBatchBroadcastConsumerListener-kafka02")
@MessageAdapter(instanceId ="kafka02", topicName = "MQS_TEST_TOPIC_BATCH_BROADCAST", isBatch = true, isBroadcast = true)
public class KafkaBatchBroadcastConsumerListener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBatchBroadcastConsumerListener.class);

    @Override
    public void process(AdapterMessage message) throws Exception {

    }

    @Override
    public void process(List<AdapterMessage> messages) throws Exception {
        logger.info("监听到批量消息，消息总数为：{}", messages.size());
        messages.forEach(message -> {
            logger.info("收到消息，TOPIC：{}，消息内容是：{}", message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
        });
    }
}
