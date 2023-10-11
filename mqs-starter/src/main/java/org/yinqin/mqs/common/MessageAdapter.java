package org.yinqin.mqs.common;


import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @description 消息适配器消费者声明注解
 * @author YinQin
 * @createTime 2023-09-28 11:44
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface MessageAdapter {

    /**
     * 组件自定义名称
     */
    String vendorName();

    /**
     * 是否为单条消息
     * 默认为批量消息
     */
    boolean isTransaction() default false;

    /**
     * 是否为广播消息
     * 默认为集群消息消息
     */
    boolean isBroadcast() default false;

    /**
     * topic名称
     */
    String topicName();

}
