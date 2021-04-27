package com.lakesidemutual.policyexpirationconstraints

import java.time.Duration
import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

object PolicyExpirationConstraints extends App {

  val kafkaStreamsConfig: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "policy-expiration-constraints-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties
  }

  val insuranceQuoteEventSerde = Serdes.serdeFrom(InsuranceQuoteEventSerde.serializer(),
    InsuranceQuoteEventSerde.deserializer())

  val windowConstraint = new WindowConstraintBuilder[String, InsuranceQuoteEvent]
    .before(((_, e) => e.isInstanceOf[InsuranceQuoteExpiredEvent], "insurance-expired"))
    .after(((_, e) => e.isInstanceOf[PolicyCreatedEvent], "policy-created"))
    .window(Duration.ofSeconds(20))
    .dropBefore

  val constraint = new ConstraintBuilder[String, InsuranceQuoteEvent, java.lang.Long]
    .windowConstraint(windowConstraint)
    .link((_, e) => e.getInsuranceQuoteRequestId)(Serdes.Long)
    .build(Serdes.String, insuranceQuoteEventSerde)

  val builder = new CStreamsBuilder()

  builder
    .stream(Set("insurance-quote-expired-events", "policy-created-events"))(Consumed.`with`(Serdes.String, insuranceQuoteEventSerde))
    .constrain(constraint)
    .to("policy-creation-expiration-events")(Produced.`with`(Serdes.String, insuranceQuoteEventSerde))

  val topology: Topology = builder.build()

  val streams = new KafkaStreams(topology, kafkaStreamsConfig)

  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(5))
  }
}