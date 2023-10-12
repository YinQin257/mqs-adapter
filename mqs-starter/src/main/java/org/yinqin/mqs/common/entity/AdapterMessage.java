package org.yinqin.mqs.common.entity;

import lombok.*;

/**
 * @author YinQin
 * @description 消息适配器抽象出来的消息实体
 * @createTime 2023-09-28 11:39
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
