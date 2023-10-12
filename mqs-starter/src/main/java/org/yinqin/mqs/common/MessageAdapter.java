package org.yinqin.mqs.common;


import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author YinQin
 * @description 消息适配器消费者声明注解
 * @createTime 2023-09-28 11:44
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface MessageAdapter {

    /**
     * topic名称
     */
    String topicName();

    /**
     * 组件自定义名称
     */
    String instanceId();

    /**
     * 是否为批量消息
     */
    boolean isBatch() default false;

    /**
     * 是否为广播消息
     * 默认为集群消息消息
     */
    boolean isBroadcast() default false;

}
