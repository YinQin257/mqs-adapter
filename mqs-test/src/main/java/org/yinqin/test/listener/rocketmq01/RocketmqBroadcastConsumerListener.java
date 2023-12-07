package org.yinqin.test.listener.rocketmq01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * rocketmq单条广播消费监听器
 *
 * @author YinQin
 * @version 1.0.5
 * @createDate 2023年10月13日
 * @since 1.0.0
 */
@Component("RocketmqBroadcastConsumerListener-rocketmq01")
@MessageAdapter(instanceId = "rocketmq01", topicName = "MQS_TEST_TOPIC_BROADCAST", isBroadcast = true)
public class RocketmqBroadcastConsumerListener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(RocketmqBroadcastConsumerListener.class);

    @Override
    public void process(AdapterMessage message)  {
        logger.info("收到消息，TOPIC：{}，消息内容是：{}", message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @Override
    public void process(List<AdapterMessage> messages)  {

    }
}
