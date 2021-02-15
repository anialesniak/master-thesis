package com.github.annterina.stream_constraints

import com.github.annterina.stream_constraints.constraints.Constraint
import org.apache.kafka.streams.scala.kstream.KStream

class CStreamsInput[K, V](stream: KStream[K, V]) {

  def constrain(constraint: Constraint[K, V]): KStream[K, V] = {
    // some filtering here and pushing downstream & return

    constraint

    stream.transform(() => new ConstraintTransformer(), "constraint.name()")
  }


}
