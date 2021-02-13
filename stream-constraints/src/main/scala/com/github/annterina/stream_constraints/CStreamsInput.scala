package com.github.annterina.stream_constraints

import org.apache.kafka.streams.scala.kstream.KStream

class CStreamsInput[K, V](stream: KStream[K, V]) {

  def constrain(constraint: Any): KStream[K, V] = {
    // some filtering here and pushing downstream & return



    stream.transform(() => new ConstraintTransformer(), "constraint.name()")
  }


}
