package com.bingo.kafka.listener;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaSendResultHandler implements ProducerListener {

	@Override
	public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
		// TODO Auto-generated method stub
		// ProducerListener.super.onSuccess(producerRecord, recordMetadata);
		System.out.println("发送成功");
	}

	@Override
	public void onError(ProducerRecord producerRecord, Exception exception) {
		// TODO Auto-generated method stub
		// ProducerListener.super.onError(producerRecord, exception);
		System.out.println("发送失败");
	}

}
