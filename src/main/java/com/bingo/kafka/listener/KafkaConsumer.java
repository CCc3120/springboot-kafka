package com.bingo.kafka.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * 
 * 消息过滤 </br>
 * 需增加containerFactory属性 指定过滤规则
 * 
 * @KafkaListener(id = "kafka_consumer1", topics = "testTopic", errorHandler =
 *                   "consumerAwareListenerErrorHandler", containerFactory =
 *                   "filterContainerFactory")
 *
 */
@Component
public class KafkaConsumer {
	@Autowired
	private ConsumerFactory consumerFactory;

	@Bean
	public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
		ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
		factory.setConsumerFactory(consumerFactory);
		// 禁止KafkaListener自启动
		// factory.setAutoStartup(false);
		// 被过滤的消息将被丢弃
		factory.setAckDiscarded(true);
		// 消息过滤策略
		factory.setRecordFilterStrategy(consumerRecord -> {
			if (Integer.parseInt(consumerRecord.value().toString()) % 2 == 0)
				return false;
			else
				return true;// 返回true消息则被过滤
		});
		return factory;
	}
}
