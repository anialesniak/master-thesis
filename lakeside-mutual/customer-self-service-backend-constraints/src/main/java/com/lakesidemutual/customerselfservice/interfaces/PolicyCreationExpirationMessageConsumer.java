package com.lakesidemutual.customerselfservice.interfaces;

import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteEvent;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteExpiredEvent;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteRequestAggregateRoot;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.PolicyCreatedEvent;
import com.lakesidemutual.customerselfservice.infrastructure.InsuranceQuoteRequestRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * PolicyCreationExpirationMessageConsumer is a Spring component that consumes PolicyCreatedEvents
 * and InsuranceQuoteExpiredEvents as they arrive through the Kafka topic.
 * It processes these events by updating the status of the corresponding insurance quote requests.
 * */
@Component
public class PolicyCreationExpirationMessageConsumer {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private InsuranceQuoteRequestRepository insuranceQuoteRequestRepository;

	@KafkaListener(topics = "${policyCreationExpirationEvent.topicName}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "policyCreationExpirationListenerFactory")
	public void receiveEvent(final InsuranceQuoteEvent event) {
		logger.info("A new policy creation or expiration event has been received.");
		
		final Long id = event.getInsuranceQuoteRequestId();
		final Optional<InsuranceQuoteRequestAggregateRoot> insuranceQuoteRequestOpt = insuranceQuoteRequestRepository.findById(id);

		if(!insuranceQuoteRequestOpt.isPresent()) {
			logger.error("Unable to process the event with an invalid insurance quote request id.");
			return;
		}

		final InsuranceQuoteRequestAggregateRoot insuranceQuoteRequest = insuranceQuoteRequestOpt.get();

		if (event instanceof InsuranceQuoteExpiredEvent) {
			InsuranceQuoteExpiredEvent insuranceQuoteExpiredEvent = (InsuranceQuoteExpiredEvent) event;
			insuranceQuoteRequest.markQuoteAsExpired(insuranceQuoteExpiredEvent.getDate());
			logger.info("The insurance quote for insurance quote request " + insuranceQuoteRequest.getId() + " has expired.");
		} else if (event instanceof PolicyCreatedEvent) {
			PolicyCreatedEvent policyCreatedEvent = (PolicyCreatedEvent) event;
			insuranceQuoteRequest.finalizeQuote(policyCreatedEvent.getPolicyId(), policyCreatedEvent.getDate());
			logger.info("The policy for for insurance quote request " + insuranceQuoteRequest.getId() + " has been created.");
		}

		insuranceQuoteRequestRepository.save(insuranceQuoteRequest);
	}
}