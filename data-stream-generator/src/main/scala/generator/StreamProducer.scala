package generator

import java.util.Properties

import org.slf4j.LoggerFactory

/**
 * Produces a stream on Kafka
 * Two possible configurations:
 * - policy
 * - insurance
 */
object StreamProducer extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val kafkaProperties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", ConfigUtils.kafkaBootstrapServers)
  kafkaProperties.setProperty("policy.topic", ConfigUtils.policyTopic)

  val publisher: Publisher = {
    if (ConfigUtils.mode == "policy") {
      new PolicyPublisher(kafkaProperties)
    } else if (ConfigUtils.mode == "insurance") {
      new InsuranceQuotePublisher(kafkaProperties)
    } else {
      throw new RuntimeException(s"Unsupported app mode ${ConfigUtils.mode}.")
    }
  }

  publisher.publish()
}