package org.yinqin.test.listener.rocketmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.yinqin.mqs.common.MessageAdapter;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @description
 * @author YinQin
 * @createTime 2023-10-10 10:44
 */

@Component
@MessageAdapter(instanceId = "kafka33",topicName = "MQS_TEST_TOPIC")
public class ConsumerListener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);


    @Override
    public void process(AdapterMessage message) throws Exception {
        logger.info("收到消息，TOPIC：{}，消息内容是：{}", message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @Override
    public void process(List<AdapterMessage> messages) throws Exception {

    }
}
