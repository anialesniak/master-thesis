name := "policy-expiration-constraints"

version := "0.1.0"

scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
  "com.github.annterina"         %%   "stream-constraints"         %   "0.4.0",
  "org.apache.kafka"              %   "kafka-streams"              %   "2.7.0",
  "org.apache.kafka"             %%   "kafka-streams-scala"        %   "2.7.0",
  "org.apache.kafka"              %   "kafka-clients"              %   "2.7.0",

  "org.springframework.hateoas"   %   "spring-hateoas"             %   "1.3.0",
  "org.microservice-api-patterns" %   "domaindrivendesign-library" %   "0.2.4",

  "org.scalatest"                %%   "scalatest"                  %   "3.2.2" % Test,
  "org.apache.kafka"              %   "kafka-streams-test-utils"   %   "2.7.0" % Test
)
