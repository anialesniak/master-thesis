package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Terminal
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext

class TerminalTransformer[K, V, L](terminals: Set[Terminal[K, V]]) extends Transformer[K, V, KeyValue[K, V]] {

  override def init(context: ProcessorContext): Unit = ???

  override def transform(key: K, value: V): KeyValue[K, V] = ???

  override def close(): Unit = ???
}
