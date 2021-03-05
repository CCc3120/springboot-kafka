package com.bingo.kafka.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 定时启动、停止监听器 </br>
 * ① 禁止监听器自启动； </br>
 * 
 * ② 创建两个定时任务，一个用来在指定时间点启动定时器，另一个在指定时间点停止定时器；</br>
 */
@Component
@EnableScheduling
public class CronTimer {
	/**
     * @KafkaListener注解所标注的方法并不会在IOC容器中被注册为Bean，
     * 而是会被注册在KafkaListenerEndpointRegistry中，
     * 而KafkaListenerEndpointRegistry在SpringIOC中已经被注册为Bean
     **/
    @Autowired
    private KafkaListenerEndpointRegistry registry;

	// @Autowired
	// private ConsumerFactory consumerFactory;

	// 监听器容器工厂(设置禁止KafkaListener自启动) 消息过滤策略中已设置
//	@Bean
//	public ConcurrentKafkaListenerContainerFactory delayContainerFactory() {
//		ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
//		container.setConsumerFactory(consumerFactory);
//		// 禁止KafkaListener自启动
//		container.setAutoStartup(false);
//		return container;
//	}

 // 定时启动监听器
    @Scheduled(cron = "0 42 11 * * ? ")
    public void startListener() {
        System.out.println("启动监听器...");
        // "timingConsumer"是@KafkaListener注解后面设置的监听器ID,标识这个监听器
		if (!registry.getListenerContainer("kafka_consumer1").isRunning()) {
			registry.getListenerContainer("kafka_consumer1").start();
        }
        //registry.getListenerContainer("timingConsumer").resume();
    }

    // 定时停止监听器
    @Scheduled(cron = "0 45 11 * * ? ")
    public void shutDownListener() {
        System.out.println("关闭监听器...");
		registry.getListenerContainer("kafka_consumer1").pause();
    }

    
}
