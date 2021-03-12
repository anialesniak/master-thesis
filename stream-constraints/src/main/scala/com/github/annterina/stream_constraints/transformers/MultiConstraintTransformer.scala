package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.{Logger, LoggerFactory}

class MultiConstraintTransformer[K, V, L](constraints: Set[Constraint[K, V, L]]) extends Transformer[K, V, KeyValue[K, V]] {

  private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(key: K, value: V): KeyValue[K, V] = ???

  override def close(): Unit = {}

}