package org.yinqin.mqs.rocketmq.MessageListener;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.entity.AdapterMessage;
import org.yinqin.mqs.common.handler.MessageHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * &#064;description:
 * &#064;author: YinQin
 * &#064;date: 2023-10-09 18:44
 */
public class BatchMessageListener implements MessageListenerConcurrently {

    private final Logger logger = LoggerFactory.getLogger(BatchMessageListener.class);

    private final Map<String, MessageHandler> messageHandlers;

    public BatchMessageListener(Map<String, MessageHandler> messageHandlers) {
        this.messageHandlers = messageHandlers;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (msgs.isEmpty()) return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        MessageExt firstMsg = msgs.get(0); // 获取第一条消息
        List<AdapterMessage> messages = new ArrayList<>();
        msgs.forEach(msg -> {
            AdapterMessage message = AdapterMessage.builder()
                    .topic(msg.getTopic())
                    .tag(msg.getTags())
                    .bizKey(msg.getKeys())
                    .body(msg.getBody())
                    .msgId(msg.getMsgId())
                    .originMessage(msg)
                    .consumeTimes(msg.getReconsumeTimes())
                    .build();
            message.setOriginMessage(msg);
            message.setMsgId(msg.getMsgId());
            message.setConsumeTimes(msg.getReconsumeTimes());
            messages.add(message);
        });
        try {
            messageHandlers.get(firstMsg.getTopic()).process(messages);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }
}
