package org.yinqin.mqs.rocketmq.MessageListener;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.yinqin.mqs.common.handler.MessageHandler;
import org.yinqin.mqs.common.entity.AdapterMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * &#064;description:
 * &#064;author: YinQin
 * &#064;date: 2023-10-09 19:08
 */
public class MessageListener implements MessageListenerConcurrently {

    private final Map<String, MessageHandler> transactionHandlers;
    public MessageListener(Map<String, MessageHandler> messageHandlers) {
        this.transactionHandlers = messageHandlers;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (msgs.isEmpty()) return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        MessageExt firstMsg = msgs.get(0); //每次只拉取一条
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
            for (AdapterMessage msg : messages) transactionHandlers.get(firstMsg.getTopic()).process(msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

    }
}
