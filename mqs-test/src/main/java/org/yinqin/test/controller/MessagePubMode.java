package org.yinqin.test.controller;

/**
 * 消息发送方式常量类
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年11月20日
 * @since 1.0.0
 */
public interface MessagePubMode {
    String SYNC = "sync";
    String ASYNC = "async";
    String ONE_WAY = "oneWay";
}
