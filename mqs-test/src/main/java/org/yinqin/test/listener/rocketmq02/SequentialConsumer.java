package org.yinqin.test.listener.rocketmq02;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单例顺序消费器-饿汉式
 *
 * @author YinQin
 * @createDate 2023年11月30日
 * @since
 */
public class SequentialConsumer {

    @Getter
    public static final SequentialConsumer instance = new SequentialConsumer();

    @Getter
    private AtomicInteger consumerId = new AtomicInteger(0);
    @Getter
    private AtomicInteger producerId = new AtomicInteger(0);

    private SequentialConsumer() {

    }

}
