package com.github.annterina.stream_constraints

import com.github.annterina.stream_constraints.constraints.PrerequisiteConstraint
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class PrerequisiteConstraintTransformer[K, V, L](constraint: PrerequisiteConstraint[K, V, L]) extends Transformer[K, V, KeyValue[K, V]] {

  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(key: K, value: V): KeyValue[K, V] = {
    val checkStore = context.getStateStore[KeyValueStore[L, Boolean]](constraint.toString + "@PrerequisiteCheck")
    val bufferStore = context.getStateStore[KeyValueStore[L, KeyValue[K, V]]](constraint.toString + "@BufferStore")
    val link = linkValue(key, value)

    if (constraint.atLeastOnce.apply(value)) {
      checkStore.put(link, true)
      context.forward(key, value)

      val buffered: KeyValue[K, V] = bufferStore.get(link)
      if (buffered != null) {
        context.forward(buffered.key, buffered.value)
        bufferStore.delete(link)
      }
    } else if (constraint.before.apply(value)) {
      val prerequisiteSeen: Boolean = checkStore.get(link)
      if (prerequisiteSeen) context.forward(key, value)
      else {
        bufferStore.put(link, KeyValue.pair(key, value))
      }
    } else {
      context.forward(key, value)
    }

    null
  }

  override def close(): Unit = {}

  private def linkValue(key: K, value: V): L = {
    constraint.link match {
      case Left(link)  => link.apply(key)
      case Right(link) => link.apply(value)
    }
  }
}