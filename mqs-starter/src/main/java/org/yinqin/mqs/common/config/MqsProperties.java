package org.yinqin.mqs.common.config;

import lombok.*;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.ClientConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 消息中间件自动装配配置类
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年10月13日
 * @since 1.0.0
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
     * 配置集合
     */
    private Map<String, AdapterProperties> adapter = new LinkedHashMap<>();

    /**
     * 消息中间件实例配置类
     *
     * @author YinQin
     * @version 1.0.3
     * @createDate 2023年10月13日
     * @since 1.0.0
     */
    @Data
    public static class AdapterProperties {

        /**
         * 消息中间件厂商名称
         */
        private String vendorName;

        /**
         * 生产组或消费组名称
         * 批量消费组（groupName）、单条消费组（groupName + ‘_TRAN’）、广播消费组（groupName + ‘BROADCAST’）
         */
        private String groupName;

        /**
         * 消费者启动开关
         */
        private boolean consumerEnabled = false;

        /**
         * 生产者启动开关
         */
        private boolean producerEnabled = false;

        /**
         * 消费组配置类
         */
        private ConvertProperties group = new ConvertProperties();

        /**
         * topic配置类
         */
        private ConvertProperties topic = new ConvertProperties();

        /**
         * rocketmq配置类
         */
        private CustomRocketmqProperties rocketmq = new CustomRocketmqProperties();

        /**
         * kafka配置类
         */
        private CustomKafkaProperties kafka = new CustomKafkaProperties();

        /**
         * 转换配置类
         *
         * @author YinQin
         * @version 1.0.4
         * @createDate 2023年11月20日
         * @since 1.0.0
         */
        @Data
        public static class ConvertProperties {

            /**
             * 前缀
             */
            private String prefix;

            /**
             * 后缀
             */
            private String suffix;

            /**
             * 是否小写转大写
             */
            private boolean isLowerToUpper = false;

            /**
             * 是否大写转小写
             */
            private boolean isUpperToLower = false;

            /**
             * 是否下划线转中划线
             */
            private boolean isUnderScoreToHyphen = false;

            /**
             * 是否中划线转下划线
             */
            private boolean isHyphenToUnderScore = false;
        }

        /**
         * rocketmq配置类
         *
         * @author YinQin
         * @version 1.0.3
         * @createDate 2023年10月13日
         * @since 1.0.0
         */
        @Data
        public static class CustomRocketmqProperties {

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
             *
             * @see ClientConfig
             */
            private ClientConfig clientConfig;

            /**
             * acl访问控制类，继承SessionCredentials
             *
             * @author YinQin
             * @version 1.0.3
             * @createDate 2023年10月13日
             * @see org.apache.rocketmq.acl.common.SessionCredentials
             * @since 1.0.0
             */
            @EqualsAndHashCode(callSuper = true)
            @Data
            public static class Acl extends SessionCredentials {

                /**
                 * acl访问控制开关
                 */
                private boolean enabled;
            }
        }

        /**
         * kafka配置类
         *
         * @author YinQin
         * @version 1.0.3
         * @createDate 2023年10月13日
         * @since 1.0.0
         */
        @Data
        public static class CustomKafkaProperties {

            /**
             * kafka其他源生配置项，可自行参考官网配置
             *
             * @see org.apache.kafka.clients.consumer.ConsumerConfig
             */
            private Properties clientConfig;

            /**
             * 拉取消息线程池配置
             *
             * @see PollTaskConfig
             */
            private PollTaskConfig pollTaskConfig = new PollTaskConfig();

            /**
             * 拉取消息间隔时间，单位：毫秒
             */
            private int interval = 100;

            /**
             * 拉取消息线程池配置类
             *
             * @author YinQin
             * @version 1.0.3
             * @createDate 2023年10月13日
             * @since 1.0.0
             */
            @Data
            public static class PollTaskConfig {

                /**
                 * 核心线程数
                 */
                private int corePoolSize = 3;

                /**
                 * 最大线程数
                 */
                private int maxPoolSize = 3;

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

}
