package org.yinqin.mqs.common.entity;

public class MessageSendResult {

    private int status;

    private Throwable throwable;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public MessageSendResult(int status, Throwable throwable) {
        this.status = status;
        this.throwable = throwable;
    }

    public MessageSendResult() {
    }

    public static enum MessageSendResultStatus {
        SEND_OK,
        FLUSH_DISK_TIMEOUT,
        FLUSH_SLAVE_TIMEOUT,
        SLAVE_NOT_AVAILABLE;
    }
}
