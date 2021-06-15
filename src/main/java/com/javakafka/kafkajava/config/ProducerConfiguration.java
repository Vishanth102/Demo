package com.javakafka.kafkajava.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;


@Configuration

public class ProducerConfiguration {

	private static final String KAFKA_BROKER = "localhost:9092";
	
	private Logger logger = LoggerFactory.getLogger(ProducerConfiguration.class);

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigurations());
	}

	@Bean
	public Map<String, Object> producerConfigurations() {
		Map<String, Object> configurations = new HashMap<>();
		configurations.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
		//Only one in-flight messages per Kafka broker connection
        // - max.in.flight.requests.per.connection (default 5)
		configurations.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1);
		 //Only retry after one second.
		configurations.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);
		  //Set the number of retries - retries
		configurations.put(ProducerConfig.RETRIES_CONFIG, 3);
		configurations.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5 );
		 //Request timeout - request.timeout.ms
		configurations.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
		configurations.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configurations.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		 try (AdminClient client = AdminClient.create(configurations)) {
	         client.listTopics(new ListTopicsOptions().timeoutMs(5000)).listings().get();
	     } catch (ExecutionException | InterruptedException ex) {
	     	logger.error("Kafka is not available, timed out after {} ms", 5000);
		}
		return configurations;
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}
