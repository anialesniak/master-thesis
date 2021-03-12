package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.PrerequisiteConstraint
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.{Logger, LoggerFactory}

class PrerequisiteConstraintTransformer[K, V, L](constraint: PrerequisiteConstraint[K, V, L]) extends Transformer[K, V, KeyValue[K, V]] {

  private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(key: K, value: V): KeyValue[K, V] = {
    val checkStore = context.getStateStore[KeyValueStore[L, Short]](constraint.toString + "@PrerequisiteCheck")
    val bufferStore = context.getStateStore[KeyValueStore[L, KeyValue[K, V]]](constraint.toString + "@BufferStore")
    val link = constraint.link.apply(key, value)

    if (constraint.atLeastOnce.apply(key, value)) {
      checkStore.put(link, 1)
      logger.info(s"FORWARDING: ${key}")
      context.forward(key, value)

      val buffered: Option[KeyValue[K, V]] = Option(bufferStore.get(link))
      if (buffered.nonEmpty) {
        logger.info(s"FORWARDING: ${buffered.get.key}")
        context.forward(buffered.get.key, buffered.get.value)
        bufferStore.delete(link)
      }
    } else if (constraint.before.apply(key, value)) {
      val prerequisiteSeen = Option(checkStore.get(link))
      if (prerequisiteSeen.nonEmpty) {
        logger.info(s"FORWARDING: ${key}")
        context.forward(key, value)
      }
      else {
        bufferStore.put(link, KeyValue.pair(key, value))
      }
    } else {
      logger.info(s"FORWARDING: ${key}")
      context.forward(key, value)
    }

    null
  }

  override def close(): Unit = {}
}