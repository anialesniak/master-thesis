package com.github.annterina.stream_constraints.example

import java.time.Duration
import java.util.Properties

import com.github.annterina.stream_constraints.{CStreamsBuilder, ConstrainedKStream}
import com.github.annterina.stream_constraints.constraints.{Constraint, ConstraintBuilder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.slf4j.{Logger, LoggerFactory}

object ExampleApplication extends App {

  private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val kafkaStreamsConfig: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.docker:9092")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties
  }


  val constraint: Constraint[String, String, String] = new ConstraintBuilder[String, String, String]
    .atLeastOnce(value => value.split("_")(1) == "created") // TODO .withLink
    .before(value => value.split("_")(1) == "updated") // TODO .withLink
    .valueLink(v => v.split("_").head)(Serdes.String)
    .build(Serdes.String, Serdes.String)

  val builder = new CStreamsBuilder()

  builder
    .stream("topic")(Consumed.`with`(Serdes.String, Serdes.String))
    .filter((k, _) => !k.startsWith("a"))
    .constrain(constraint)
    .to("output-topic")(Produced.`with`(Serdes.String, Serdes.String))

  val topology: Topology = builder.build()

  logger.info(topology.describe().toString)
  val streams = new KafkaStreams(topology, kafkaStreamsConfig)

  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(5))
  }

}
