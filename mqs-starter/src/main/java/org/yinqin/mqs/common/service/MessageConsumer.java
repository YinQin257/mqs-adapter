package org.yinqin.mqs.common.service;

import org.springframework.beans.factory.DisposableBean;

/**
 * @author YinQin
 * @description 消息适配器定义的顶级消费者接口
 * @createTime 2023-09-28 11:44
 */
public interface MessageConsumer extends DisposableBean {

    /**
     * 消费券启动方法
     *
     * @throws Exception none
     */
    void start() throws Exception;
}
