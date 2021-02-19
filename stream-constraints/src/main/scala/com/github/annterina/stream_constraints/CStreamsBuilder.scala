package com.github.annterina.stream_constraints

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed

class CStreamsBuilder  {

  private val builder = new StreamsBuilder()

  def stream[K, V](topic: String)(implicit consumed: Consumed[K, V]): ConstrainedKStream[K, V] = {
    new ConstrainedKStream[K, V](builder.stream(topic), builder)
  }

  def build(): Topology = {
    builder.build()
  }

}
