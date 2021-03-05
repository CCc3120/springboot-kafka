package com.bingo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;

@Configuration
public class KafkaConfig {

	@Bean(name = "consumerAwareListenerErrorHandler")
	public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {

		return (message, exception, consumer) -> {
			System.out.println("消费异常：" + message.getPayload());
			return null;
		};
	}

	// 使用@Bean注解创建Topic
	@Bean
//	@Primary同类型的Bean时优先使用该Bean
	public NewTopic initialTopic() {
		// 创建分区为2副本为1的topic
		return new NewTopic("testTopic", 1, (short) 1);
	}

	// 修改后|分区数只能增大不能减小
//	@Bean
//	public NewTopic initialTopic() {
//		return new NewTopic("topic.quick.initial", 11, (short) 1);
//	}

//	@Bean
//	public ProducerFactory<Integer, String> producerFactory() {
//		DefaultKafkaProducerFactory factory = new DefaultKafkaProducerFactory<>(senderProps());
//		factory.transactionCapable();
//		factory.setTransactionIdPrefix("tran-");
//		return factory;
//	}
//
//	@Bean
//	public KafkaTransactionManager transactionManager(ProducerFactory producerFactory) {
//		KafkaTransactionManager manager = new KafkaTransactionManager(producerFactory);
//		return manager;
//	}
}
