package com.lakesidemutual.customerselfservice.constraints

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.Duration
import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import com.lakesidemutual.customerselfservice.constraints.serde.InsuranceQuoteEventSerde
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.{InsuranceQuoteEvent, InsuranceQuoteExpiredEvent, PolicyCreatedEvent}
import com.lakesidemutual.customerselfservice.infrastructure.InsuranceQuoteRequestRepository
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream.Consumed
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class PolicyCreationExpirationMessageConsumer {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  val STREAMS_APP_NAME = "policy-creation-expiration-streams"
  val INSURANCE_QUOTE_EXPIRED_TOPIC_NAME = "insurance-quote-expired-events"
  val POLICY_CREATED_TOPIC_NAME = "policy-created-events"

  val insuranceQuoteEventSerde: Serde[InsuranceQuoteEvent] = Serdes.serdeFrom(InsuranceQuoteEventSerde.serializer(),
    InsuranceQuoteEventSerde.deserializer())

  @Autowired
  var insuranceQuoteRequestRepository: InsuranceQuoteRequestRepository = null

  @Bean
  def kafkaStreams(): KafkaStreams = {
    val kafkaStreamsConfig: Properties = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "policy-expiration-constraints-application")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      properties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      properties
    }

    val insuranceQuoteEventSerde = Serdes.serdeFrom(InsuranceQuoteEventSerde.serializer(),
      InsuranceQuoteEventSerde.deserializer())

    val windowConstraint = new WindowConstraintBuilder[String, InsuranceQuoteEvent]
      .before(((_, e) => e.isInstanceOf[InsuranceQuoteExpiredEvent], "insurance-quote-expired"))
      .after(((_, e) => e.isInstanceOf[PolicyCreatedEvent], "policy-created"))
      .window(Duration.ofMillis(2000))
      .dropBefore

    val constraint = new ConstraintBuilder[String, InsuranceQuoteEvent, java.lang.Long]
      .windowConstraint(windowConstraint)
      .link((_, e) => e.getInsuranceQuoteRequestId)(Serdes.Long)
      .build(Serdes.String, insuranceQuoteEventSerde)

    val builder = new CStreamsBuilder()

    builder
      .stream(Set("insurance-quote-expired-events", "policy-created-events"))(Consumed.`with`(Serdes.String, insuranceQuoteEventSerde))
      .constrain(constraint)
      .foreach((_, event) => handleEvent(event))

    val topology: Topology = builder.build()

    val streams = new KafkaStreams(topology, kafkaStreamsConfig)

    streams.start()
    streams
  }

  private def handleEvent(event: InsuranceQuoteEvent): Unit =  {
//  		val id = event.getInsuranceQuoteRequestId
//  		val insuranceQuoteRequestOpt = insuranceQuoteRequestRepository.findById(id);
//
//  		if(!insuranceQuoteRequestOpt.isPresent) {
//  			logger.error("Unable to process the event with an invalid insurance quote request id.")
//  			return
//  		}
//
//  		val insuranceQuoteRequest = insuranceQuoteRequestOpt.get()

  		event match {
        case insuranceQuoteExpiredEvent: InsuranceQuoteExpiredEvent =>
          logger.info("A new insurance expiration event has been received.")

          val line = insuranceQuoteExpiredEvent.getInsuranceQuoteRequestId.toString + " " +
            insuranceQuoteExpiredEvent.getDate.getTime + " " + System.currentTimeMillis() + " InsuranceQuoteExpiredEvent\n"

          Files.write(Paths.get("received-insurance-events-sc2000.txt"), line.getBytes, StandardOpenOption.APPEND)

        //insuranceQuoteRequest.markQuoteAsExpired(insuranceQuoteExpiredEvent.getDate)
        //logger.info("The insurance quote for insurance quote request " + insuranceQuoteRequest.getId + " has expired.");

        case policyCreatedEvent:  PolicyCreatedEvent =>
          logger.info("A new policy creation event has been received.")

          val line = policyCreatedEvent.getInsuranceQuoteRequestId.toString + " " +
            policyCreatedEvent.getDate.getTime + " " + System.currentTimeMillis() + " PolicyCreatedEvent\n"

          Files.write(Paths.get("received-insurance-events-sc2000.txt"), line.getBytes, StandardOpenOption.APPEND)

          //insuranceQuoteRequest.finalizeQuote(policyCreatedEvent.getPolicyId, policyCreatedEvent.getDate)
          //logger.info("The policy for for insurance quote request " + insuranceQuoteRequest.getId + " has been created.");
      }

  		//insuranceQuoteRequestRepository.save(insuranceQuoteRequest);
  	}
}
