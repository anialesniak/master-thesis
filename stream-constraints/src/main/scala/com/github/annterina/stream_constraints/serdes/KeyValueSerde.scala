package com.github.annterina.stream_constraints.serdes

import java.nio.ByteBuffer

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KeyValue

class KeyValueSerde[K, V](val keySerde: Serde[K], val valueSerde: Serde[V]) extends Serde[KeyValue[K, V]] {

  val IntSize: Int = 4

  override def serializer(): Serializer[KeyValue[K, V]] = (topic: String, data: KeyValue[K, V]) => {
    val keyBytes = keySerde.serializer.serialize(null, data.key)
    val valueBytes = valueSerde.serializer.serialize(null, data.value)

    ByteBuffer.allocate(IntSize + keyBytes.length + valueBytes.length)
      .putInt(keyBytes.length)
      .put(keyBytes)
      .put(valueBytes)
      .array
  }

  override def deserializer(): Deserializer[KeyValue[K, V]] = (topic: String, data: Array[Byte]) => {
    val buffer: ByteBuffer = ByteBuffer.wrap(data)
    val keyLength: Int = buffer.getInt
    val keyBytes: Array[Byte] = new Array[Byte](keyLength)
    buffer.get(keyBytes)
    val valueBytes: Array[Byte] = new Array[Byte](buffer.remaining)
    buffer.get(valueBytes)

    KeyValue.pair(
      keySerde.deserializer().deserialize(null, keyBytes),
      valueSerde.deserializer().deserialize(null, valueBytes)
    )
  }
}