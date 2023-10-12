package org.yinqin.mqs.common.entity;


/**
 * @author YinQin
 * @description 消息适配器定义的异步消息回调
 * @createTime 2023-09-28 11:39
 */
public interface MessageCallback {

    /**
     * 消息发送成功回调
     */
    void onSuccess();

    /**
     * 消息发送失败回调
     *
     * @param throwable none
     */
    void onError(Throwable throwable);
}
