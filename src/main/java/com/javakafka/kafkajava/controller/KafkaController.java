package com.javakafka.kafkajava.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.javakafka.kafkajava.consumer.TopicConsumer;

@RestController
public class KafkaController {

	private KafkaTemplate<String, String> template;
	@Autowired
	TopicConsumer topicConsumer;

	public KafkaController(KafkaTemplate<String, String> template) {
		this.template = template;
	}

	@GetMapping("/kafka/produce")
	public String produce(@RequestParam String message) {
		template.send("Par-Academika-Price_Availability-DEV", message);
		System.out.println("Message is sent");
		System.out.println("Message is sent");
		return "message sent to the topic";
	}

	@GetMapping("/kafka/messages")
	public List<String> getMessages() {
		return topicConsumer.getMessages();
	}
}