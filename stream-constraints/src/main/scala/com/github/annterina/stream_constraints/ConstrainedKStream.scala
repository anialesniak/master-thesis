package com.github.annterina.stream_constraints

import java.lang

import com.github.annterina.stream_constraints.constraints.{Constraint, MultiPrerequisiteConstraint, PrerequisiteConstraint}
import com.github.annterina.stream_constraints.serdes.KeyValueSerde
import com.github.annterina.stream_constraints.transformers.{MultiConstraintTransformer, PrerequisiteConstraintTransformer}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores, TimestampedKeyValueStore}

class ConstrainedKStream[K, V, L](inner: KStream[K, V], builder: StreamsBuilder) {

  def constrain(constraint: Constraint[K, V, L]): ConstrainedKStream[K, V, L] = {
    val keyValueBufferStore = bufferStore(constraint)
    builder.addStateStore(keyValueBufferStore)

    val checkKeyValueStore = checkStore(constraint)
    builder.addStateStore(checkKeyValueStore)

    constraint match {
      case prerequisite: PrerequisiteConstraint[K, V, L] =>
        new ConstrainedKStream(inner.transform(() =>
          new PrerequisiteConstraintTransformer(prerequisite),
          keyValueBufferStore.name(),
          checkKeyValueStore.name()),
          builder)
    }
  }

  def constrain(constraints: MultiPrerequisiteConstraint[K, V, L]): ConstrainedKStream[K, V, L] = ???

  def map[KR, VR](mapper: (K, V) => (KR, VR)): ConstrainedKStream[KR, VR, L] =
    new ConstrainedKStream(inner.map[KR, VR](mapper), builder)

  def filter(predicate: (K, V) => Boolean): ConstrainedKStream[K, V, L] =
    new ConstrainedKStream(inner.filter(predicate), builder)

  def to(topic: String)(implicit produced: Produced[K, V]): Unit =
    inner.to(topic)(produced)


  private def bufferStore(constraint: Constraint[K, V, L]): StoreBuilder[KeyValueStore[L, KeyValue[K, V]]] = {
    val storeName = constraint.toString + "@BufferStore" //TODO better name
    val storeSupplier = Stores.persistentKeyValueStore(storeName)
    Stores.keyValueStoreBuilder(
      storeSupplier,
      constraint.linkSerde,
      new KeyValueSerde[K, V](constraint.keySerde, constraint.valueSerde))
  }

  private def checkStore(constraint: Constraint[K, V, L]): StoreBuilder[KeyValueStore[L, lang.Short]] = {
    val checkStoreName = constraint.toString + "@PrerequisiteCheck" //TODO better name
    val checkStoreSupplier = Stores.persistentKeyValueStore(checkStoreName)
    Stores.keyValueStoreBuilder(
      checkStoreSupplier,
      constraint.linkSerde,
      Serdes.Short)
  }

  private def timestampedBufferStore(constraint: Constraint[K, V, L]): StoreBuilder[TimestampedKeyValueStore[L, KeyValue[K, V]]] = {
    val storeName = constraint.toString + "@TimestampedBufferStore" //TODO better name
    val storeSupplier = Stores.persistentTimestampedKeyValueStore(storeName)
    Stores.timestampedKeyValueStoreBuilder(
      storeSupplier,
      constraint.linkSerde,
      new KeyValueSerde[K, V](constraint.keySerde, constraint.valueSerde))
  }
}