package org.yinqin.mqs.common.entity;

import lombok.Data;

/**
 * 消息发送结果实体
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @since 1.0.0
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
