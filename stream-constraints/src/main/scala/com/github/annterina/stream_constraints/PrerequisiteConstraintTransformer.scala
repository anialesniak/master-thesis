package com.github.annterina.stream_constraints

import com.github.annterina.stream_constraints.constraints.PrerequisiteConstraint
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext

class PrerequisiteConstraintTransformer[K, V](constraint: PrerequisiteConstraint[K, V]) extends Transformer[K, V, KeyValue[K, V]] {

  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(readOnlyKey: K, value: V): KeyValue[K, V] = {



    new KeyValue[K, V](readOnlyKey, value)
  }

  override def close(): Unit = ???
}