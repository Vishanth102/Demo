package com.javakafka.kafkajava.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

@Component
public class TopicConsumer {
	public List<String> msgList = new ArrayList<>();
	
	@KafkaListener(topics = "Par-Academika-Price_Availability-DEV", groupId = "kafka-sandbox")
	public void listen(String message) {
		synchronized (msgList) {
			msgList.add(message);
		}
	}

	public List<String> getMessages() {
		return msgList;
	}
}