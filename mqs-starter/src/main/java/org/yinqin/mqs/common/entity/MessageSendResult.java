package org.yinqin.mqs.common.entity;

import lombok.Data;

/**
 * @description 消息发送结果
 * @author YinQin
 * @createTime 2023-09-28 11:39
 */
@Data
public class MessageSendResult {

    /**
     * 消息发送状态
     */
    private int status;

    /**
     * 消息发送失败异常
     */
    private Throwable throwable;
}
