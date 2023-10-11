package org.yinqin.mqs.common.handler;

import org.yinqin.mqs.common.entity.AdapterMessage;

import java.util.List;

/**
 * @description 消费者消息处理顶级接口
 * @author YinQin
 * @createTime 2023-09-28 11:44
 */
public interface MessageHandler {

	/**
	 * 单条或广播消息处理方法
	 * @param message 消息
	 * @throws Exception 异常
	 */
	void process(AdapterMessage message) throws Exception;

	/**
	 * 批量消息处理方法
	 * @param messages 消息
	 * @throws Exception 异常
	 */
	void process(List<AdapterMessage> messages) throws Exception;
}
