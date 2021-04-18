package com.lakesidemutual.customerselfservice.interfaces;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteRequestAggregateRoot;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.PolicyCreatedEvent;
import com.lakesidemutual.customerselfservice.infrastructure.InsuranceQuoteRequestRepository;

/**
 * PolicyCreatedMessageConsumer is a Spring component that consumes PolicyCreatedEvents
 * as they arrive through the ActiveMQ message queue. It processes these events by updating
 * the status of the corresponding insurance quote requests.
 * */
@Component
public class PolicyCreatedMessageConsumer {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private InsuranceQuoteRequestRepository insuranceQuoteRequestRepository;

	@KafkaListener(topics = "${policyCreatedEvent.topicName}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "policyCreatedListenerFactory")
	public void receivePolicyCreatedEvent(final PolicyCreatedEvent policyCreatedEvent) {
		logger.info("A new PolicyCreatedEvent has been received.");
		
		final Long id = policyCreatedEvent.getInsuranceQuoteRequestId();
		final Optional<InsuranceQuoteRequestAggregateRoot> insuranceQuoteRequestOpt = insuranceQuoteRequestRepository.findById(id);

		if(!insuranceQuoteRequestOpt.isPresent()) {
			logger.error("Unable to process a policy created event with an invalid insurance quote request id.");
			return;
		}

		final InsuranceQuoteRequestAggregateRoot insuranceQuoteRequest = insuranceQuoteRequestOpt.get();
		insuranceQuoteRequest.finalizeQuote(policyCreatedEvent.getPolicyId(), policyCreatedEvent.getDate());
		logger.info("The insurance quote for insurance quote request " + insuranceQuoteRequest.getId() + " has expired.");
		insuranceQuoteRequestRepository.save(insuranceQuoteRequest);
	}
}