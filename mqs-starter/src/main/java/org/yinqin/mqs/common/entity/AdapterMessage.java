package org.yinqin.mqs.common.entity;

import lombok.*;

/**
 * &#064;description: 消息实体
 * &#064;author: YinQin
 * &#064;date: 2023-09-28 11:39
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AdapterMessage {
    private String topic;
    private String tag;
    private String bizKey;
    private byte[] body;
    private Long processTime; //处理时间
    private Long deliverTime; //定时消息
    private String msgId;
    private Integer partition;
    private long offset;
    private int consumeTimes;
    private Object originMessage;
}
