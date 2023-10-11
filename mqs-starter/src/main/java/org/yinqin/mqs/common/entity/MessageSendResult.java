package org.yinqin.mqs.common.entity;

import lombok.Data;

@Data
public class MessageSendResult {

    private int status;

    private Throwable throwable;

    public MessageSendResult() {
    }
}
