package com.github.annterina.stream_constraints.serdes

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.KeyValue

import scala.collection.mutable.ListBuffer

class KeyValuesSerde[K, V](val keyValueSerde: Serde[KeyValue[K, V]]) extends Serde[List[KeyValue[K, V]]] {

  val IntSize: Int = 4

  override def serializer(): Serializer[List[KeyValue[K, V]]] =  (topic: String, data: List[KeyValue[K, V]]) => {
    if (data == null)
      null
    else {
      val baos = new ByteArrayOutputStream()
      val out = new DataOutputStream(baos)

      out.writeInt(data.size)
      data.foreach(keyValue => {
        val bytes = keyValueSerde.serializer.serialize(topic, keyValue)
        out.writeInt(bytes.length)
        out.write(bytes)
      })

      baos.toByteArray
    }
  }

  override def deserializer(): Deserializer[List[KeyValue[K, V]]] = (topic: String, data: Array[Byte]) => {
    val dis = new DataInputStream(new ByteArrayInputStream(data))

    val size = dis.readInt()
    val list: ListBuffer[KeyValue[K, V]] = ListBuffer.empty[KeyValue[K, V]]

    for (i <- 0 until size) {
      val entrySize = dis.readInt()
      val payload = dis.readNBytes(entrySize)
      list.addOne(keyValueSerde.deserializer.deserialize(topic, payload))
    }

    list.toList
  }
}
