package com.lakesidemutual.policymanagement.infrastructure;

import org.microserviceapipatterns.domaindrivendesign.DomainEvent;
import org.microserviceapipatterns.domaindrivendesign.InfrastructureService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.lakesidemutual.policymanagement.domain.insurancequoterequest.InsuranceQuoteExpiredEvent;
import com.lakesidemutual.policymanagement.domain.insurancequoterequest.InsuranceQuoteResponseEvent;
import com.lakesidemutual.policymanagement.domain.insurancequoterequest.PolicyCreatedEvent;

/**
 * CustomerSelfServiceMessageProducer is an infrastructure service class that is used to notify the Customer Self-Service Backend
 * when Lakeside Mutual has responded to a customer's insurance quote request (InsuranceQuoteResponseEvent) or when an insurance quote
 * has expired (InsuranceQuoteExpiredEvent). These events are transmitted via an ActiveMQ message queue.
 * */
@Component
public class CustomerSelfServiceMessageProducer implements InfrastructureService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${insuranceQuoteResponseEvent.topicName}")
	private String quoteResponseTopic;

	@Value("${insuranceQuoteExpiredEvent.topicName}")
	private String quoteExpiredTopic;

	@Value("${policyCreatedEvent.topicName}")
	private String policyCreatedTopic;

	@Autowired
	private KafkaTemplate<String, DomainEvent> kafkaTemplate;

	public void sendInsuranceQuoteResponseEvent(InsuranceQuoteResponseEvent event) {
		try {
			kafkaTemplate.send(quoteResponseTopic, event.getInsuranceQuoteRequestId().toString(), event);
			logger.info("Successfully sent an insurance quote response to the Customer Self-Service backend.");
		} catch(Exception exception) {
			logger.error("Failed to send an insurance quote response to the Customer Self-Service backend.", exception);
		}
	}

	public void sendInsuranceQuoteExpiredEvent(InsuranceQuoteExpiredEvent event) {
		try {
			kafkaTemplate.send(quoteExpiredTopic, event.getInsuranceQuoteRequestId().toString(), event);
			logger.info("Successfully sent an insurance quote expired event to the Customer Self-Service backend.");
		} catch(Exception exception) {
			logger.error("Failed to send an insurance quote expired event to the Customer Self-Service backend.", exception);
		}
	}

	public void sendPolicyCreatedEvent(PolicyCreatedEvent event) {
		try {
			kafkaTemplate.send(policyCreatedTopic, event.getInsuranceQuoteRequestId().toString(), event);
			logger.info("Successfully sent an policy created event to the Customer Self-Service backend.");
		} catch(Exception exception) {
			logger.error("Failed to send an policy created event to the Customer Self-Service backend.", exception);
		}
	}
}