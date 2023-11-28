package org.yinqin.mqs.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rocketmq.common.message.Message;
import org.yinqin.mqs.common.Constants;
import org.yinqin.mqs.common.config.MqsProperties.AdapterProperties;
import org.yinqin.mqs.common.entity.AdapterMessage;

/**
 * 转换工具类
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年11月20日
 * @since 1.0.4
 */
public class ConvertUtil {
    /**
     * topic名称或者消费组名称转换
     *
     * @param name 消费组名称或topic名称
     * @param convertProperties 转换配置
     * @return 转换后的名称
     */
    public static String convertName(String name, AdapterProperties.ConvertProperties convertProperties) {
        if (convertProperties == null) return name;
        if (StringUtils.isNotBlank(convertProperties.getPrefix()) &&!name.startsWith(convertProperties.getPrefix()))
            name = convertProperties.getPrefix().concat(name);
        if (StringUtils.isNotBlank(convertProperties.getSuffix()) &&!name.endsWith(convertProperties.getSuffix()))
            name = name.concat(convertProperties.getSuffix());
        if (convertProperties.isLowerToUpper()) name = name.toUpperCase();
        if (convertProperties.isUpperToLower()) name = name.toLowerCase();
        if (convertProperties.isUnderScoreToHyphen()) name = name.replace(Constants.UNDER_SCORE, Constants.HYPHEN);
        if (convertProperties.isHyphenToUnderScore()) name = name.replace(Constants.HYPHEN, Constants.UNDER_SCORE);
        return name;
    }

    /**
     * 适配器消息转rocketmq原生消息
     * @param message 消息
     * @return rocketmq原生消息
     */
    public static Message AdapterMessageToRocketmqMessage(AdapterMessage message, AdapterProperties.ConvertProperties topicProperties) {
        return new Message(ConvertUtil.convertName(message.getTopic(),topicProperties), message.getTag(), message.getBizKey(), message.getBody());
    }

    /**
     * 适配器消息转kafka原生消息
     * @param message 消息
     * @return kafka原生消息
     */
    public static ProducerRecord<String, byte[]> AdapterMessageToKafkaMessage(AdapterMessage message, AdapterProperties.ConvertProperties topicProperties) {
        return new ProducerRecord<>(ConvertUtil.convertName(message.getTopic(),topicProperties), null, message.getBizKey(), message.getBody(), null);
    }
}
