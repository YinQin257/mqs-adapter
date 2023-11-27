package org.yinqin.mqs.kafka.consumer;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.service.MessageConsumer;
import org.yinqin.mqs.kafka.PollWorker;

/**
 * 自定义kafka消费者
 *
 * @author YinQin
 * @version 1.0.5
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.service.MessageConsumer
 * @since 1.0.0
 */
public class CustomKafkaConsumer implements MessageConsumer {

    private final Logger logger = LoggerFactory.getLogger(CustomKafkaConsumer.class);

    /**
     * 实例ID
     */
    private final String instanceId;

    /**
     * 拉取消息工作线程
     */
    @Getter
    private final PollWorker pollWorker;

    public CustomKafkaConsumer(String instanceId, PollWorker pollWorker) {
        this.instanceId = instanceId;
        this.pollWorker = pollWorker;
    }

    /**
     * 异步启动拉取消息工作线程
     */
    @Override
    public void start() {
        new Thread(pollWorker).start();
    }

    /**
     * 停止拉取消息工作线程
     */
    @Override
    public void destroy() {
        pollWorker.shutdown();
        logger.info("实例：{} 消费者停止成功", instanceId);
    }

}
