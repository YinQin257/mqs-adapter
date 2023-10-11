package org.yinqin.mqs.common.service;

import org.springframework.beans.factory.DisposableBean;

/**
 * &#064;description: 消息消费者SPI
 * &#064;author: YinQin
 * &#064;date: 2023-09-28 11:44
 */
public interface MessageConsumer extends DisposableBean {
    void start() throws Exception;
}
