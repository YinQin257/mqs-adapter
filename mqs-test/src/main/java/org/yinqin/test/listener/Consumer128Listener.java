package org.yinqin.test.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.MessageAdapter;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @description
 * @author YinQin
 * @createTime 2023-10-10 10:44
 */

@Component
@MessageAdapter(vendorName = "rocketmq128",topicName = "MQS_TEST_TOPIC", isBatch = true)
public class Consumer128Listener implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(Consumer128Listener.class);


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
