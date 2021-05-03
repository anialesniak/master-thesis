package generator

import java.util.Properties

import org.slf4j.LoggerFactory

/**
 * Produces a stream on Kafka
 * Two possible configurations:
 * - periodic-burst: publishes a load of messages each minute
 * - constant-rate: publishes a constant rate of messages (each 100ms)
 */
object StreamProducer extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val kafkaProperties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", ConfigUtils.kafkaBootstrapServers)
  kafkaProperties.setProperty("policy.topic", ConfigUtils.policyTopic)

  val publisher: Publisher = {
    if (ConfigUtils.mode == "constant-rate") {
      new ConstantRatePublisher(kafkaProperties)
    } else {
      throw new RuntimeException(s"Unsupported app mode ${ConfigUtils.mode}.")
    }
  }

  publisher.publish()
}