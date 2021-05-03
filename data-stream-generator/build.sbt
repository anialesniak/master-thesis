name := "data-stream-generator"
organization := "com.github.annterina"
version := "0.1.0"

scalaVersion := "2.13.4"

resolvers ++= Seq(
	"confluent" at "https://packages.confluent.io/maven/"
)

val kafkaStreamsDependencies = Seq(
	"org.apache.kafka"  %      "kafka-clients"                 %   "2.7.0",
	"org.apache.kafka"  %      "kafka-streams"                 %   "2.7.0",
	"org.apache.kafka"  %%     "kafka-streams-scala"           %   "2.7.0",
	"org.apache.kafka"  %      "kafka-streams-test-utils"      %   "2.7.0" % Test
)

libraryDependencies ++= kafkaStreamsDependencies ++ Seq(
	"ch.qos.logback"    %      "logback-classic"               %   "1.2.3",
	"com.typesafe"      %      "config"                        %   "1.4.1")

