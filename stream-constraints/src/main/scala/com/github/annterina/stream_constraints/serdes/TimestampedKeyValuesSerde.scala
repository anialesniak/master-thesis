package com.github.annterina.stream_constraints.serdes

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde

import scala.collection.mutable.ListBuffer

class TimestampedKeyValuesSerde[K, V](val keyValueSerde: Serde[KeyValue[K, V]]) extends Serde[List[ValueAndTimestamp[KeyValue[K, V]]]] {

  val IntSize: Int = 4
  val vtSerde = new ValueAndTimestampSerde(keyValueSerde)

  override def serializer(): Serializer[List[ValueAndTimestamp[KeyValue[K, V]]]] =  (topic: String, data: List[ValueAndTimestamp[KeyValue[K, V]]]) => {
    val baos = new ByteArrayOutputStream()
    val out = new DataOutputStream(baos)

    out.writeInt(data.size)
    data.foreach(keyValue => {
      val bytes = vtSerde.serializer.serialize(topic, keyValue)
      out.writeInt(bytes.length)
      out.write(bytes)
    })

    baos.toByteArray
  }

  override def deserializer(): Deserializer[List[ValueAndTimestamp[KeyValue[K, V]]]] = (topic: String, data: Array[Byte]) => {
    val dis = new DataInputStream(new ByteArrayInputStream(data))

    val size = dis.readInt()
    val list: ListBuffer[ValueAndTimestamp[KeyValue[K, V]]] = ListBuffer.empty[ValueAndTimestamp[KeyValue[K, V]]]

    for (i <- 0 until size) {
      val entrySize = dis.readInt()
      val payload = dis.readNBytes(entrySize)
      list.addOne(vtSerde.deserializer.deserialize(topic, payload))
    }

    list.toList
  }
}
