package com.bingo.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.bingo.kafka.model.User;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	@RequestMapping(value = "/send1")
	public String send1() throws Exception {
		User user = new User();
		user.setAge(12);
		user.setId(123123123l);
		user.setName("张三");
		kafkaTemplate.send("testTopic", "这是一条测试kafka消息" + JSON.toJSONString(user));
//		kafkaTemplate.send("testTopic", "这是一条测试kafka消息" + JSON.toJSONString(user)).get();
		return "ok";
	}

	@RequestMapping(value = "/send2")
	public String send2() {
		User user = new User();
		user.setAge(12);
		user.setId(123123123l);
		user.setName("张三");
		kafkaTemplate.send("testTopic", "这是一条测试kafka消息" + JSON.toJSONString(user)).addCallback(success -> {
			// 消息发送到的topic
			String topic = success.getRecordMetadata().topic();
			// 消息发送到的分区
			int partition = success.getRecordMetadata().partition();
			// 消息在分区内的offset
			long offset = success.getRecordMetadata().offset();
			System.out.println("发送消息成功:" + topic + "-" + partition + "-" + offset);
		}, failure -> {
			System.out.println("发送消息失败:" + failure.getMessage());
		});

		return "ok";
	}

	@RequestMapping(value = "/send3")
	public String send3() {
		User user = new User();
		user.setAge(12);
		user.setId(123123123l);
		user.setName("张三");
		kafkaTemplate.send("testTopic", "这是一条测试kafka消息" + JSON.toJSONString(user)).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
			@Override
			public void onFailure(Throwable ex) {
				System.out.println("发送消息失败：" + ex.getMessage());
			}

			@Override
			public void onSuccess(SendResult<String, Object> result) {
				System.out.println("发送消息成功：" + result.getRecordMetadata().topic() + "-"
						+ result.getRecordMetadata().partition() + "-" + result.getRecordMetadata().offset());
			}
		});
		return "ok";
	}

	@RequestMapping(value = "/send4")
	public String send4() {
		boolean flag = false;
		// 声明事务：后面报错消息不会发出去
		kafkaTemplate.executeInTransaction(operations -> {
			operations.send("testTopic", 0, "testTopic", "1");
			// operations.send("testTopic", 0, "testTopic", "2");
			// operations.send("testTopic", "test executeInTransaction");
			if (flag) {
				throw new RuntimeException("fail");
			} else {
				return "ok";
			}
		});

//		kafkaTemplate.executeInTransaction(new OperationsCallback<String, Object, Object>() {
//			@Override
//			public Object doInOperations(KafkaOperations<String, Object> operations) {
//				operations.send("testTopic", "test executeInTransaction");
//				throw new RuntimeException("fail");
//				// return null;
//			}
//		});

		// 不声明事务：后面报错但前面消息已经发送成功了
//		kafkaTemplate.send("topic1", "test executeInTransaction");
//		throw new RuntimeException("fail");

		return "ok";
	}

}
