package com.github.annterina.stream_constraints

import com.github.annterina.stream_constraints.constraints.{Constraint, PrerequisiteConstraint}
import com.github.annterina.stream_constraints.serdes.KeyValueSerde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import org.apache.kafka.streams.state.Stores

class ConstrainedKStream[K, V](inner: KStream[K,V], builder: StreamsBuilder) {

  def constrain(constraint: Constraint[K, V]): ConstrainedKStream[K, V] = {
    val storeName = constraint.toString //TODO better name
    val storeSupplier = Stores.persistentKeyValueStore(storeName)
    val keyValueStore = Stores.keyValueStoreBuilder(
      storeSupplier,
      constraint.linkSerde,
      new KeyValueSerde[K, V](constraint.keySerde, constraint.valueSerde))
    builder.addStateStore(keyValueStore)

    constraint match {
      case prerequisite: PrerequisiteConstraint[K, V] =>
        new ConstrainedKStream(inner.transform(() => new PrerequisiteConstraintTransformer(prerequisite), storeName), builder)
    }
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): ConstrainedKStream[KR, VR] =
    new ConstrainedKStream(inner.map[KR, VR](mapper), builder)

  def filter(predicate: (K, V) => Boolean): ConstrainedKStream[K, V] =
    new ConstrainedKStream(inner.filter(predicate), builder)

  def to(topic: String)(implicit produced: Produced[K, V]): Unit =
    inner.to(topic)(produced)

}