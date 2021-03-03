package com.github.annterina.stream_constraints.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object OrderEventSerde extends Serde[OrderEvent]  {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def serializer(): Serializer[OrderEvent] = (topic: String, data: OrderEvent) => {
    mapper.writeValueAsBytes(data)
  }

  override def deserializer(): Deserializer[OrderEvent] = (topic: String, data: Array[Byte]) => {
    mapper.readValue(data, classOf[OrderEvent])
  }


}
