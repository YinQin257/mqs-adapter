package org.yinqin.mqs.common.entity;

import lombok.*;

/**
 * 消息适配器抽象出来的消息实体
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @see org.apache.kafka.clients.consumer.ConsumerRecord
 * @see org.apache.rocketmq.common.message.MessageExt
 * @since 1.0.0
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AdapterMessage {

    /**
     * 消息主题
     */
    private String topic;

    /**
     * 消息标签
     * 注：kafka不支持该属性
     */
    private String tag;

    /**
     * 键
     */
    private String bizKey;

    /**
     * 消息体，为了统一类型，统一项消息体定义为字节类型
     */
    private byte[] body;

    /**
     * 消息处理时间
     */
    private Long processTime;

    /**
     * 消息标识
     */
    private String msgId;

    /**
     * 消费时间
     */
    private int consumeTimes;

    /**
     * 原始消息
     */
    private Object originMessage;
}
