package org.yinqin.mqs.common.manager;

import org.yinqin.mqs.common.service.MessageProducer;

import java.util.LinkedHashMap;

/**
 * 生产者管理器
 *
 * @author YinQin
 * @version 1.0.4
 * @createDate 2023年10月13日
 * @see java.util.LinkedHashMap
 * @since 1.0.0
 */
public class ProducerManager extends LinkedHashMap<String, MessageProducer> {

    /**
     * 获取默认生产者实例
     *
     * @return 生产者
     */
    public MessageProducer getDefaultInstance() {
        return this.get("default");
    }
}
