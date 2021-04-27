package com.lakesidemutual.customerselfservice.interfaces.configuration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * The MessagingConfiguration class configures the Kafka message broker. This broker is used
 * to send events to the Risk Management Server when a policy changes.
 * */
@Configuration
public class KafkaMessagingConfiguration {

	@Value(value = "${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${insuranceQuoteRequestEvent.topicName}")
	private String insuranceQuoteRequestEventTopic;

	@Value("${customerDecisionEvent.topicName}")
	private String customerDecisionEventTopic;

	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		return new KafkaAdmin(configs);
	}

	@Bean
	public NewTopic insuranceQuoteRequestTopic() {
		return new NewTopic(insuranceQuoteRequestEventTopic, 1, (short) 1);
	}

	@Bean
	public NewTopic customerDecisionTopic() {
		return new NewTopic(customerDecisionEventTopic, 1, (short) 1);
	}
}