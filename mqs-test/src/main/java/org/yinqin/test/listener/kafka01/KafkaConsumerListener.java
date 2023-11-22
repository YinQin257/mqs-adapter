package org.yinqin.test.listener.kafka01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * kafka单条集群消费监听器
 *
 * @author YinQin
 * @version 1.0.5
 * @createDate 2023年11月20日
 * @since 1.0.3
 */
@Component("KafkaConsumerListener-default")
@MessageAdapter(topicName = "MQS_TEST_TOPIC")
public class KafkaConsumerListener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerListener.class);

    @Override
    public void process(AdapterMessage message) throws Exception {
        logger.info("收到消息，TOPIC：{}，消息内容是：{}", message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @Override
    public void process(List<AdapterMessage> messages) throws Exception {

    }
}
