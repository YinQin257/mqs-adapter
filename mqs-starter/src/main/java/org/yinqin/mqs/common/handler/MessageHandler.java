package org.yinqin.mqs.common.handler;

import org.yinqin.mqs.common.entity.AdapterMessage;

import java.util.List;

/**
 * &#064;description: 消息处理器接口
 * &#064;author: YinQin
 * &#064;date: 2023-09-28 11:44
 */
public interface MessageHandler {

	void process(AdapterMessage message) throws Exception;

	void process(List<AdapterMessage> messages) throws Exception;
}
