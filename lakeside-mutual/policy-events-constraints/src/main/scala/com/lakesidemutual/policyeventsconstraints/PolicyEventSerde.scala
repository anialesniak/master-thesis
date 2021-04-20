package com.lakesidemutual.policyeventsconstraints

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object PolicyEventSerde extends Serde[PolicyDomainEvent]  {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def serializer(): Serializer[PolicyDomainEvent] = (topic: String, data: PolicyDomainEvent) => {
    mapper.writeValueAsBytes(data)
  }

  override def deserializer(): Deserializer[PolicyDomainEvent] = (topic: String, data: Array[Byte]) => {
    mapper.readValue(data, classOf[PolicyDomainEvent])
  }
}