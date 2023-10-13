package org.yinqin.mqs.common.entity;


/**
 * 消息适配器定义的异步消息回调
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @since 1.0.0
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
