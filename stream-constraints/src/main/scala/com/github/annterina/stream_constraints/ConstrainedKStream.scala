package com.github.annterina.stream_constraints

import com.github.annterina.stream_constraints.constraints.{Constraint, MultiPrerequisiteConstraint}
import com.github.annterina.stream_constraints.serdes.{GraphSerde, KeyValueSerde}
import com.github.annterina.stream_constraints.transformers.graphs.ConstraintNode
import com.github.annterina.stream_constraints.transformers.MultiConstraintTransformer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores, TimestampedKeyValueStore}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.mutable.Graph

import scala.collection.mutable

class ConstrainedKStream[K, V, L](inner: KStream[K, V], builder: StreamsBuilder) {

  def constrain(constraint: Constraint[K, V, L]): ConstrainedKStream[K, V, L] = {
    constraint match {
      case multiConstraint: MultiPrerequisiteConstraint[K, V, L] => {
        val graphs = graphStore(multiConstraint)
        builder.addStateStore(graphs)

        val names = multiConstraint.constraints
          .foldLeft(mutable.Set.empty[String])((names, prerequisite) =>  {
            val beforeName = prerequisite.before._2
            if (!names.contains(beforeName)) {
              names.add(beforeName)
              builder.addStateStore(timestampedBufferStore(multiConstraint, beforeName))
            }

            val afterName = prerequisite.after._2
            if (!names.contains(afterName)) {
              names.add(afterName)
              builder.addStateStore(timestampedBufferStore(multiConstraint, afterName))
            }
            names
          })

        names.add(graphs.name())

        new ConstrainedKStream(inner.transform(() => new MultiConstraintTransformer(multiConstraint),
          names.toList:_*), builder)
      }
    }
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): ConstrainedKStream[KR, VR, L] =
    new ConstrainedKStream(inner.map[KR, VR](mapper), builder)

  def filter(predicate: (K, V) => Boolean): ConstrainedKStream[K, V, L] =
    new ConstrainedKStream(inner.filter(predicate), builder)

  def to(topic: String)(implicit produced: Produced[K, V]): Unit =
    inner.to(topic)(produced)


  private def graphStore(constraint: Constraint[K, V, L]): StoreBuilder[KeyValueStore[L, Graph[ConstraintNode, DiEdge]]] = {
    val graphStoreName = "Graph"
    val graphStoreSupplier = Stores.persistentKeyValueStore(graphStoreName)
    Stores.keyValueStoreBuilder(graphStoreSupplier, constraint.linkSerde, GraphSerde)
  }

  private def timestampedBufferStore(constraint: Constraint[K, V, L], name: String): StoreBuilder[TimestampedKeyValueStore[L, KeyValue[K, V]]] = {
    val storeSupplier = Stores.persistentTimestampedKeyValueStore(name)
    Stores.timestampedKeyValueStoreBuilder(
      storeSupplier,
      constraint.linkSerde,
      new KeyValueSerde[K, V](constraint.keySerde, constraint.valueSerde))
  }
}