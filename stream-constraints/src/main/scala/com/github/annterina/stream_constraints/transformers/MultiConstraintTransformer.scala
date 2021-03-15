package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.MultiPrerequisiteConstraint
import com.github.annterina.stream_constraints.serdes.GraphSerde
import com.github.annterina.stream_constraints.transformers.graphs.ConstraintNode
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, TimestampedKeyValueStore, ValueAndTimestamp}
import org.slf4j.{Logger, LoggerFactory}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.io.json.JsonGraph
import scalax.collection.mutable.Graph

import scala.collection.mutable

class MultiConstraintTransformer[K, V, L](constraint: MultiPrerequisiteConstraint[K, V, L]) extends Transformer[K, V, KeyValue[K, V]] {

  private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var context: ProcessorContext = _
  var graph: Graph[ConstraintNode, DiEdge] = Graph.empty

  override def init(context: ProcessorContext): Unit = {
    this.context = context

    constraint.constraints.foreach(c => {
      val (before, after) = (ConstraintNode(c.before._2, seen = false, buffered = false),
        ConstraintNode(c.after._2, seen = false, buffered = false))
      this.graph.addEdge(before, after)(DiEdge)
    })

    // TODO check if it is acyclic
  }

  override def transform(key: K, value: V): KeyValue[K, V] = {
    val graphStore = context.getStateStore[KeyValueStore[L, Graph[ConstraintNode, DiEdge]]]("Graph")
    val link = constraint.link.apply(key, value)

    // check if there is ANY constraint specified for this event
    if (!constraint.constraints.exists(p => p.before._1.apply(key, value) || p.after._1.apply(key, value))) {
      context.forward(key, value)
      return null
    }

    graphStore.putIfAbsent(link, this.graph.clone())
    val graph: Graph[ConstraintNode, DiEdge] = graphStore.get(link)

    val constraintNode = graph.nodes.find(node => constraint.names(node.value.name).apply(key, value))
    val before: Set[graph.NodeT] = constraintNode.get.diPredecessors

    if (before.isEmpty || before.forall(node => node.value.seen)) {
      context.forward(key, value)
      constraintNode.get.value.seen = true

      // get possible buffered
      val nodeOrdering: graph.NodeOrdering = graph.NodeOrdering((node1, node2) => node1.incoming.size.compare(node2.incoming.size))
      val successors = graph.innerNodeTraverser(constraintNode.get).withOrdering(nodeOrdering)

      var bufferedToPublish = Seq.empty[ValueAndTimestamp[KeyValue[K, V]]]
      successors.toList.tail.foreach(node => {
        if (node.value.buffered && node.diPredecessors.forall(node => node.value.seen)) {
          val buffered = bufferStore(node.value.name).get(link)
          bufferedToPublish :+= buffered
          node.value.seen = true
          node.value.buffered = false
        }
      })

      bufferedToPublish
        .sortBy(_.timestamp())
        .map(_.value())
        .foreach(record => context.forward(record.key, record.value))

      graphStore.put(link, graph)
    } else {
      // buffer this event
      bufferStore(constraintNode.get.value.name)
        .put(link, ValueAndTimestamp.make(KeyValue.pair(key, value), context.timestamp()))

      constraintNode.get.value.buffered = true
      graphStore.put(link, graph)
    }

    logger.info(graph.toJson(GraphSerde.descriptor))

    null
  }

  override def close(): Unit = {}

  private def bufferStore(name: String): TimestampedKeyValueStore[L, KeyValue[K, V]] = {
    context.getStateStore[TimestampedKeyValueStore[L, KeyValue[K, V]]](name)
  }

}