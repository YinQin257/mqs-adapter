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
 * &#064;description: rocketmq配置类
 * &#064;author: YinQin
 * &#064;date: 2023-09-28 14:29
 */

@ConfigurationProperties(MqsProperties.PREFIX)
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MqsProperties {
    public static final String PREFIX = "mqs";


    private Map<String, RocketmqProperties> rocketmq = new LinkedHashMap<>();

    private Map<String, KafkaProperties> kafka = new LinkedHashMap<>();

    @Data
    public static class RocketmqProperties {
        private String groupName;
        private String accessKey;
        private String secretKey;
        private int consumeMessageBatchMaxSize = 1;
        private int consumeThreadMax = 1;
        private int consumeThreadMin = 1;
        private int pullThresholdForQueue = 1000;
        private int consumeConcurrentlyMaxSpan = 500;
        private Acl acl = new Acl();
        private ClientConfig clientConfig;
        @Data
        public static class Acl extends SessionCredentials{
            private boolean enabled;
        }
    }

    @Data
    public static class KafkaProperties {
        private Properties clientConfig;
        private String groupName;
        private PollTaskConfig pollTaskConfig = new PollTaskConfig();

        @Data
        public static class PollTaskConfig {
            private int corePoolSize = 1;
            private int maxPoolSize = 20;
            private int keepAliveSeconds = 60;
            private int queueCapacity = 20;
        }
    }
}
