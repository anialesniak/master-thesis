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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * PolicyCreationExpirationMessageConsumer is a Spring component that consumes PolicyCreatedEvents
 * and InsuranceQuoteExpiredEvents as they arrive through the Kafka topic.
 * It processes these events by updating the status of the corresponding insurance quote requests.
 * */
@Component
public class PolicyCreationExpirationMessageConsumer {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	List<InsuranceQuoteEvent> queue = new LinkedList<>();

	@Autowired
	private InsuranceQuoteRequestRepository insuranceQuoteRequestRepository;

	@KafkaListener(topics = {"${policyCreatedEvent.topicName}", "${insuranceQuoteExpiredEvent.topicName}"},
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "policyCreationExpirationListenerFactory")
	public void receiveEvent(final InsuranceQuoteEvent event) {
		queue.add(event);
	}

	@Scheduled(fixedRate = 1000, initialDelay = 10000)
	private void filterAndHandleEvents() {
		logger.info("Insurance events publish initiated with " + this.queue.size() + " elements.");

		List<InsuranceQuoteEvent> buffered = this.queue;
		this.queue = new LinkedList<>();

		Map<Long, List<InsuranceQuoteEvent>> eventsPerInsuranceQuote = buffered
			.stream()
			.collect(groupingBy(InsuranceQuoteEvent::getInsuranceQuoteRequestId));

		Map<Long, List<InsuranceQuoteEvent>> filteredEventsPerInsuranceQuote = eventsPerInsuranceQuote
			.entrySet()
			.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, e -> filterInsuranceQuoteEvents(e.getValue())));

		filteredEventsPerInsuranceQuote
			.forEach((k, v) -> v.forEach(this::handleEvent));
	}

	private void handleEvent(final InsuranceQuoteEvent event) {

		if (event instanceof InsuranceQuoteExpiredEvent) {
			InsuranceQuoteExpiredEvent insuranceQuoteExpiredEvent = (InsuranceQuoteExpiredEvent) event;

			String line = event.getInsuranceQuoteRequestId().toString() + " " +
					insuranceQuoteExpiredEvent.getDate().getTime() + " "
					+ System.currentTimeMillis() + " InsuranceQuoteExpiredEvent\n";
			try {
				Files.write(Paths.get("received-insurance-events.txt"), line.getBytes(), StandardOpenOption.APPEND);
			} catch (IOException e) {}

			//insuranceQuoteRequest.markQuoteAsExpired(insuranceQuoteExpiredEvent.getDate());
			//logger.info("The insurance quote for insurance quote request " + insuranceQuoteExpiredEvent.getInsuranceQuoteRequestId() + " has expired.");
		} else if (event instanceof PolicyCreatedEvent) {
			PolicyCreatedEvent policyCreatedEvent = (PolicyCreatedEvent) event;

			String line = event.getInsuranceQuoteRequestId().toString() + " " +
					policyCreatedEvent.getDate().getTime() + " "
					+ System.currentTimeMillis() + " PolicyCreatedEvent\n";
			try {
				Files.write(Paths.get("received-insurance-events.txt"), line.getBytes(), StandardOpenOption.APPEND);
			} catch (IOException e) {}

			//insuranceQuoteRequest.finalizeQuote(policyCreatedEvent.getPolicyId(), policyCreatedEvent.getDate());
			//logger.info("The policy for for insurance quote request " + policyCreatedEvent.getInsuranceQuoteRequestId() + " has been created.");
		}

		//insuranceQuoteRequestRepository.save(insuranceQuoteRequest);
	}

	private List<InsuranceQuoteEvent> filterInsuranceQuoteEvents(List<InsuranceQuoteEvent> events) {
		if (events.size() > 1 && events.get(0) instanceof InsuranceQuoteExpiredEvent) {
			return events.subList(1, 2);
		}
		return events;
	}
}