package org.yinqin.mqs.common.service;

import org.springframework.beans.factory.DisposableBean;

/**
 * @description 消息适配器定义的顶级消费者接口
 * @author YinQin
 * @createTime 2023-09-28 11:44
 */
public interface MessageConsumer extends DisposableBean {

    /**
     * 消费券启动方法
     * @throws Exception none
     */
    void start() throws Exception;
}
