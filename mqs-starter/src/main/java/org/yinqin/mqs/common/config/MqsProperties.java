package org.yinqin.mqs.common.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.ClientConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @description 消息中间件自动装配配置类
 * @author YinQin
 * @createTime 2023-09-28 14:29
 */

@ConfigurationProperties(MqsProperties.PREFIX)
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MqsProperties {

    /**
     * 配置项前缀
     */
    public static final String PREFIX = "mqs";

    /**
     * rocketmq配置集合
     */
    private Map<String, RocketmqProperties> rocketmq = new LinkedHashMap<>();

    /**
     * kafka配置集合
     */
    private Map<String, KafkaProperties> kafka = new LinkedHashMap<>();

    /**
     * @description rocketmq配置类
     * @author YinQin
     * @createTime 2023-09-28 14:29
     */
    @Data
    public static class RocketmqProperties {

        /**
         * rocketmq生产组或消费组名称
         * 批量消费组（groupName）、单条消费组（groupName + ‘_TRAN’）、广播消费组（groupName + ‘BROADCAST’）
         */
        private String groupName;

        /**
         * 消费者启动开关
         */
        private boolean consumerEnabled;

        /**
         * 生产者启动开关
         */
        private boolean producerEnabled;

        /**
         * rocketmq acl访问控制
         * 可配置acl开关、accessKey、secretKey
         */
        private Acl acl = new Acl();

        /**
         * 批量消费最大数量，建议不超过32
         */
        private int consumeMessageBatchMaxSize = 1;

        /**
         * 消费线程最大数量
         */
        private int consumeThreadMax = 1;

        /**
         * 消费线程最小数量
         */
        private int consumeThreadMin = 1;

        /**
         * 流量控制
         * 消费者本地缓存消息数超过pullThresholdForQueue时，降低拉取消息频率
         */
        private int pullThresholdForQueue = 1000;

        /**
         * 流量控制
         * 消费者本地缓存消息跨度超过consumeConcurrentlyMaxSpan时，降低拉取消息频率
         */
        private int consumeConcurrentlyMaxSpan = 500;

        /**
         * rocketmq其他源生配置项，可自行参考官网配置
         */
        private ClientConfig clientConfig;

        /**
         * @description acl访问控制，继承SessionCredentials
         * @author YinQin
         * @createTime 2023-09-28 14:29
         */
        @Data
        public static class Acl extends SessionCredentials{

            /**
             * acl访问控制开关
             */
            private boolean enabled;
        }
    }

    /**
     * @description kafka配置类
     * @author YinQin
     * @createTime 2023-09-28 14:29
     */
    @Data
    public static class KafkaProperties {

        /**
         * 消费者启动开关
         */
        private boolean consumerEnabled;

        /**
         * 生产者启动开关
         */
        private boolean producerEnabled;

        /**
         * kafka其他源生配置项，可自行参考官网配置
         */
        private Properties clientConfig;

        /**
         * 生产组或消费组名称
         * 批量消费组（groupName）、单条消费组（groupName + ‘_TRAN’）、广播消费组（groupName + ‘BROADCAST’）
         */
        private String groupName;

        /**
         * 拉取消息线程池配置
         */
        private PollTaskConfig pollTaskConfig = new PollTaskConfig();

        /**
         * @description 拉取消息线程池配置类
         * @author YinQin
         * @createTime 2023-09-28 14:29
         */
        @Data
        public static class PollTaskConfig {

            /**
             * 核心线程数
             */
            private int corePoolSize = 1;

            /**
             * 最大线程数
             */
            private int maxPoolSize = 20;

            /**
             * 空闲线程最大存活时间
             */
            private int keepAliveSeconds = 60;

            /**
             * 等待队列大小
             */
            private int queueCapacity = 20;
        }
    }
}
