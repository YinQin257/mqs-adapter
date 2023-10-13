package org.yinqin.mqs.common.service;

import org.springframework.beans.factory.DisposableBean;

/**
 * 消息适配器定义的顶级消费者接口
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @see org.springframework.beans.factory.DisposableBean
 * @since 1.0.0
 */
public interface MessageConsumer extends DisposableBean {

    /**
     * 消费组启动方法
     *
     * @throws Exception none
     */
    void start() throws Exception;
}
