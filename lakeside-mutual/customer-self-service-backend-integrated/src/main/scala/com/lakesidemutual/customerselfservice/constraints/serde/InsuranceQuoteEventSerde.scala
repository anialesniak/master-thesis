package com.lakesidemutual.customerselfservice.constraints.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.{InsuranceQuoteEvent, InsuranceQuoteExpiredEvent, PolicyCreatedEvent}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object InsuranceQuoteEventSerde extends Serde[InsuranceQuoteEvent] {

  val EXPIRATION_TOPIC = "insurance-quote-expired-events"
  val POLICY_CREATED_TOPIC = "policy-created-events"

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def serializer(): Serializer[InsuranceQuoteEvent] = (topic: String, data: InsuranceQuoteEvent) => {
    mapper.writeValueAsBytes(data)
  }

  override def deserializer(): Deserializer[InsuranceQuoteEvent] = (topic: String, data: Array[Byte]) => {
    topic match {
      case EXPIRATION_TOPIC => mapper.readValue(data, classOf[InsuranceQuoteExpiredEvent])
      case POLICY_CREATED_TOPIC => mapper.readValue(data, classOf[PolicyCreatedEvent])
      case _ =>
        val event = mapper.readValue(data, classOf[PolicyCreatedEvent])
        if (event.getPolicyId == null) {
          mapper.readValue(data, classOf[InsuranceQuoteExpiredEvent])
        } else {
          event
        }
    }
  }
}
