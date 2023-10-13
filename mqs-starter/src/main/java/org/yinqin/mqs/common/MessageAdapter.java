package org.yinqin.mqs.common;


import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 消息适配器消费者注解
 *
 * @author YinQin
 * @version 1.0.3
 * @createDate 2023年10月13日
 * @since 1.0.0
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface MessageAdapter {

    /**
     * @return topic名称
     */
    String topicName();

    /**
     * @return 组件自定义名称
     */
    String instanceId();

    /**
     * @return 是否开启批量消费
     */
    boolean isBatch() default false;

    /**
     * @return 是否开启广播消费, 默认为集群消费模式
     */
    boolean isBroadcast() default false;

}
