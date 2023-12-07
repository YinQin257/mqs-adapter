package org.yinqin.test.listener.rocketmq02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * rocketmq批量集群消费监听器
 *
 * @author YinQin
 * @version 1.0.5
 * @createDate 2023年10月13日
 * @since 1.0.5
 */
@Component("RocketmqBatchConsumerListener-rocketmq02")
@MessageAdapter(instanceId = "rocketmq02", topicName = "MQS_TEST_TOPIC_BATCH", isBatch = true)
public class RocketmqBatchConsumerListener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(RocketmqBatchConsumerListener.class);

    @Override
    public void process(AdapterMessage message)  {

    }

    @Override
    public void process(List<AdapterMessage> messages)  {
        logger.info("监听到批量消息，消息总数为：{}", messages.size());
        messages.forEach(message -> {
            logger.info("收到消息，TOPIC：{}，消息内容是：{}", message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
        });
    }
}
