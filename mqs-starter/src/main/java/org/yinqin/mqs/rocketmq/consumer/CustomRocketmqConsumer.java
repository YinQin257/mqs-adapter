package org.yinqin.mqs.rocketmq.consumer;

import lombok.Getter;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yinqin.mqs.common.config.MqsProperties.AdapterProperties;
import org.yinqin.mqs.common.service.MessageConsumer;

/**
 * 自定义rocketmq消费者
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年10月13日
 * @see org.yinqin.mqs.common.service.MessageConsumer
 * @since 1.0.0
 */
public class CustomRocketmqConsumer implements MessageConsumer {

    private final Logger logger = LoggerFactory.getLogger(CustomRocketmqConsumer.class);

    /**
     * 实例ID
     */
    private final String instanceId;

    /**
     * rocketmq原生消费者对象
     */
    @Getter
    private final DefaultMQPushConsumer consumer;

    public CustomRocketmqConsumer(String instanceId, AdapterProperties rocketmqProperties) {
        this.instanceId = instanceId;
        String groupName = rocketmqProperties.getGroupName();
        if (rocketmqProperties.getRocketmq().getAcl().isEnabled()) {
            consumer = new DefaultMQPushConsumer(groupName, new AclClientRPCHook(rocketmqProperties.getRocketmq().getAcl()), new AllocateMessageQueueAveragely());
        } else {
            consumer = new DefaultMQPushConsumer(groupName);
        }
    }

    /**
     * 启动所有类型的消费组
     */
    @Override
    public void start() {
        try {
            consumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭所有源生rocketmq消费者
     */
    @Override
    public void destroy() {
        consumer.shutdown();
        logger.info("实例：{} 消费者停止成功", instanceId);
    }
}
