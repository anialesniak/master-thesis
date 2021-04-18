package com.lakesidemutual.policymanagement.infrastructure;

import org.microserviceapipatterns.domaindrivendesign.DomainEvent;
import org.microserviceapipatterns.domaindrivendesign.InfrastructureService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * RiskManagementMessageProducer is an infrastructure service class that is used to notify the Risk Management Server
 * about policy events (e.g., a new policy is created). These events are transmitted via an ActiveMQ message queue.
 * */
@Component
public class RiskManagementMessageProducer implements InfrastructureService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${riskmanagement.topicName}")
	private String topicName;

	@Autowired
	private KafkaTemplate<String, DomainEvent> kafkaTemplate;

	/**
	 * This method first converts the event into a JSON payload using the MappingJackson2MessageConverter that was set up in the
	 * MessagingConfiguration class. It then sends this payload to the ActiveMQ queue with the given queue name.
	 */
	public void emitEvent(DomainEvent event) {
		try {
			kafkaTemplate.send(topicName, event);
			logger.info("Successfully sent a policy event to the risk management message topic.");
		} catch(Exception exception) {
			logger.error("Failed to send a policy event to the risk management message topic.", exception);
		}
	}
}

