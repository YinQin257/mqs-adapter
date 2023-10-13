package org.yinqin.mqs.common.handler;

import org.yinqin.mqs.common.entity.AdapterMessage;

import java.util.List;

/**
 * 消费者消息处理顶级接口
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @since 1.0.0
 */
public interface MessageHandler {

    /**
     * 单条或广播消息处理方法
     *
     * @param message 消息
     * @throws Exception 异常
     */
    void process(AdapterMessage message) throws Exception;

    /**
     * 批量消息处理方法
     *
     * @param messages 消息
     * @throws Exception 异常
     */
    void process(List<AdapterMessage> messages) throws Exception;
}
