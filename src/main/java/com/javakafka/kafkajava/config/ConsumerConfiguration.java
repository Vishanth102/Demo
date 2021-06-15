
package com.javakafka.kafkajava.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class ConsumerConfiguration {
	private static final String KAFKA_BROKER = "localhost:9093";
	private static final String GROUP_ID = "kafka-sandbox";
	
	private Logger logger = LoggerFactory.getLogger(ConsumerConfiguration.class);

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigurations());
	}

	@Bean
	public Map<String, Object> consumerConfigurations() {
		Map<String, Object> configurations = new HashMap<>();
		configurations.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
		configurations.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		configurations.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configurations.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		KafkaConsumer simpleConsumer = new KafkaConsumer(configurations);
		
		 try {
			 simpleConsumer.listTopics();
	     } catch (Exception ex) {
	    	 logger.error("Consumer is not available, timed out after {} ms, \n {}", 5000,ex.getMessage());
		}
		return configurations;
	}

	@Bean
	ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}
}
