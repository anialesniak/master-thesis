package com.github.annterina.stream_constraints

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed

class CStreamsBuilder  {

  private val builder = new StreamsBuilder()

  def stream[K, V](topic: String)(implicit consumed: Consumed[K, V]): CStreamsInput[K, V] = {
    new CStreamsInput[K, V](builder.stream(topic))
  }

  def build(): Topology = {
    builder.build()
  }

}
