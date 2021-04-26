package com.github.annterina.stream_constraints

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed

class CStreamsBuilder  {

  private val builder = new StreamsBuilder()

  def stream[K, V, L](topic: String)(implicit consumed: Consumed[K, V]): ConstrainedKStream[K, V, L] = {
    new ConstrainedKStream[K, V, L](builder.stream(topic), builder)
  }

  def stream[K, V, L](topics: Set[String])(implicit consumed: Consumed[K, V]): ConstrainedKStream[K, V, L] = {
    new ConstrainedKStream[K, V, L](builder.stream(topics), builder)
  }

  def build(): Topology = {
    builder.build()
  }

}
