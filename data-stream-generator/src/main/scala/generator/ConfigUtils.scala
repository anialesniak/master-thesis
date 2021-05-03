package generator

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtils extends Serializable {
  val configProperties: Config = ConfigFactory.load("resources.conf")
  val generalConfigProps: Config = configProperties.getConfig("general")

  // General settings
  lazy val mode: String = generalConfigProps.getString("mode")
  lazy val dataPath: String = generalConfigProps.getString("data.path")

  // Kafka settings
  lazy val kafkaBootstrapServers: String = configProperties.getString("kafka.bootstrap.servers")
  lazy val policyTopic: String = configProperties.getString("kafka.policy.topic")
}
