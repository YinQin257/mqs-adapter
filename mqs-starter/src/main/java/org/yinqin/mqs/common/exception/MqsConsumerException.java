package org.yinqin.mqs.common.exception;

/**
 * 消费异常类
 *
 * @author YinQin
 * @version 1.0.8
 * @createDate 2023年11月30日
 * @since 1.0.8
 */
public class MqsConsumerException extends RuntimeException {
    public MqsConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}