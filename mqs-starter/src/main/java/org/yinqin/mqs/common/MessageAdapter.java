package org.yinqin.mqs.common;


import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(TYPE)
public @interface MessageAdapter {

    String vendorName();

    boolean isTransaction() default false;

    boolean isBroadcast() default false;

    String topicName();

}
