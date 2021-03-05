package com.bingo.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerListener {
	//, errorHandler = "consumerAwareListenerErrorHandler", containerFactory = "filterContainerFactory"
	@KafkaListener(id = "kafka_consumer1", topics = "testTopic")
	public void onMessage(List<ConsumerRecord<?, ?>> records) throws Exception {
		for (ConsumerRecord<?, ?> record : records) {
			// throw new Exception("简单消费-模拟异常");
			System.out.println("kafka接收到的消息为：" + record.topic() + "-" + record.partition() + "-" + record.value());
		}
	}

	@KafkaListener(id = "kafka_consumer2", topics = "testTopic1", groupId = "testTopic-3")
	public void onMessage2(List<ConsumerRecord<?, ?>> records) {
		for (ConsumerRecord<?, ?> record : records) {
			System.out.println("topic:" + record.topic() + "|partition:" + record.partition() + "|offset:" + record.offset() + "|key:" + record.key()
					+ "|value:" + record.value() + "|timestamp:" + record.timestamp() + "|headers:" + record.headers().toString());
		}
	}

	/**
	 * @Title 消息转发
	 * @Description 从topic1接收到的消息经过处理后转发到topic2
	 * @Param [record]
	 * @return String
	 **/
	// @KafkaListener(id = "kafka_consumer5", topics = { "testTopic" })
	// @SendTo("testTopic1")
	public String onMessage7(List<ConsumerRecord<?, ?>> records) {
		System.out.println("-forward message");
		return records.size() + "-forward message";
	}

	/**
	 * @Title 指定topic、partition、offset消费
	 * @Description 同时监听topic1和topic2，监听topic1的0号分区、topic2的 "0号和1号"
	 *              分区，指向1号分区的offset初始值为8
	 * @Author long.yuan
	 * @Date 2020/3/22 13:38
	 * @Param [record]
	 * @return void
	 **/
//	@KafkaListener(id = "kafka_consumer3", groupId = "testTopic-4", topicPartitions = {
//			@TopicPartition(topic = "topic1", partitions = { "0" }),
//			@TopicPartition(topic = "topic2", partitions = "0", partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "8"))
//	})
	public void onMessage3(List<ConsumerRecord<?, ?>> records) {
		for (ConsumerRecord<?, ?> record : records) {
			System.out.println("topic:" + record.topic() + "|partition:" + record.partition() + "|offset:" + record.offset() + "|key:" + record.key()
					+ "|value:" + record.value() + "|timestamp:" + record.timestamp() + "|headers:" + record.headers().toString());
		}
	}
}
