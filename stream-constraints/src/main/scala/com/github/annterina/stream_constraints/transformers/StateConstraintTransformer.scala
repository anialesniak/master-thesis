package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import com.github.annterina.stream_constraints.graphs.ConstraintNode
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, ValueAndTimestamp}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.mutable.Graph

import scala.collection.mutable.ListBuffer

class StateConstraintTransformer[K, V, L](constraint: Constraint[K, V, L], graphTemplate: Graph[ConstraintNode, DiEdge])
  extends Transformer[K, V, KeyValue[Redirect[K], V]] {

  var context: ProcessorContext = _
  var graphStore: KeyValueStore[L, Graph[ConstraintNode, DiEdge]] = _
  var terminatedStore: KeyValueStore[L, Long] = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context

    require(graphTemplate.isAcyclic, "The constraints cannot be mutually exclusive")

    val terminalNodes = graphTemplate.nodes.filter(_.value.terminal)
    require(terminalNodes.forall(node => node.diSuccessors.isEmpty), "Terminal nodes cannot be prerequisites")

    this.graphStore = context.getStateStore[KeyValueStore[L, Graph[ConstraintNode, DiEdge]]]("Graph")
    this.terminatedStore = context.getStateStore[KeyValueStore[L, Long]]("Terminated")
  }

  override def transform(key: K, value: V): KeyValue[Redirect[K], V] = {
    val link = constraint.link.apply(key, value)

    // check if there is ANY prerequisite constraint specified for this event
    // TODO add terminals here
    if (constraintsNotApplicable(constraint, key, value)) {
      context.forward(Redirect(key, redirect = false), value)
      return null
    }

    // check if the process for this link is terminated
    if (Option(terminatedStore.get(link)).isDefined) {
      context.forward(Redirect(key, redirect = true), value)
      return null
    }

    graphStore.putIfAbsent(link, graphTemplate.clone())
    val graph: Graph[ConstraintNode, DiEdge] = graphStore.get(link)

    val constraintNode = graph.nodes.find(node => constraint.names(node.value.name).apply(key, value))
    val before: Set[graph.NodeT] = constraintNode.get.diPredecessors

    if (before.isEmpty || before.forall(node => node.value.seen)) {
      forward(constraintNode.get.value.terminal, key, value)
      constraintNode.get.value.seen = true

      // get possible buffered
      val nodeOrdering: graph.NodeOrdering = graph.NodeOrdering((node1, node2) => node1.incoming.size.compare(node2.incoming.size))
      val successors = graph.innerNodeTraverser(constraintNode.get).withOrdering(nodeOrdering)

      val bufferedToPublish = ListBuffer.empty[(ValueAndTimestamp[KeyValue[K, V]], Boolean)]
      successors.toList.tail.foreach(node => {
        if (node.value.buffered && node.diPredecessors.forall(node => node.value.seen)) {
          val buffered = bufferStore(node.value.name).get(link)
          val zipped = buffered.zip(LazyList.continually(node.value.terminal))
          bufferedToPublish.addAll(zipped)
          bufferStore(node.value.name).delete(link)

          node.value.seen = true
          node.value.buffered = false
        }
      })

      bufferedToPublish
        .sortBy(_._1.timestamp())
        .foreach(record => forward(record._2, record._1.value().key, record._1.value().value))

      graphStore.put(link, graph)
    } else {
      // buffer this event
      val buffered = Option(bufferStore(constraintNode.get.value.name).get(link))
        .getOrElse(List.empty[ValueAndTimestamp[KeyValue[K, V]]])
      val newList = buffered
        .appended(ValueAndTimestamp.make(KeyValue.pair(key, value), context.timestamp()))
      bufferStore(constraintNode.get.value.name).put(link, newList)

      constraintNode.get.value.buffered = true
      graphStore.put(link, graph)
    }

    null
  }

  override def close(): Unit = {}

  private def bufferStore(name: String): KeyValueStore[L, List[ValueAndTimestamp[KeyValue[K, V]]]] = {
    context.getStateStore[KeyValueStore[L, List[ValueAndTimestamp[KeyValue[K, V]]]]](name)
  }

  private def constraintsNotApplicable(constraint: Constraint[K, V, L], key: K, value: V): Boolean = {
    !constraint.prerequisites.exists(p => p.before._1.apply(key, value) || p.after._1.apply(key, value))
  }

  private def forward(isTerminal: Boolean, key: K, value: V): Unit = {
    val link = constraint.link.apply(key, value)

    if (isTerminal && Option(terminatedStore.get(link)).isEmpty) {
      context.forward(Redirect(key, redirect = false), value)
      terminatedStore.put(link, context.timestamp())
      graphStore.delete(link)
    } else if (isTerminal && Option(terminatedStore.get(link)).isDefined) {
      context.forward(Redirect(key, redirect = true), value)
    } else {
      context.forward(Redirect(key, redirect = false), value)
    }
  }
}