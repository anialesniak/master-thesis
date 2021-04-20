name := "stream-constraints"
organization := "com.github.annterina"
version := "0.3.0"

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
  "ch.qos.logback"               %      "logback-classic"             %   "1.2.3",
  "org.scalatest"                %%     "scalatest"                   %   "3.2.2" % Test,
  "com.fasterxml.jackson.module" %%     "jackson-module-scala"        %   "2.12.0",
  "org.scala-graph"              %%     "graph-core"                  %   "1.13.2",
  "org.scala-graph"              %%     "graph-json"                  %   "1.13.0",
  "org.scala-graph"              %%     "graph-dot"                   %   "1.13.0",
  "guru.nidi"                    %      "graphviz-java"               %   "0.18.1",
  "net.liftweb"                  %%     "lift-json"                   %   "3.4.3"
)
