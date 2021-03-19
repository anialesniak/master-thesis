package com.github.annterina.stream_constraints.transformers

import java.io.File

import com.github.annterina.stream_constraints.constraints.MultiPrerequisiteConstraint
import com.github.annterina.stream_constraints.transformers.graphs.ConstraintNode
import guru.nidi.graphviz.engine.{Format, Graphviz}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, ValueAndTimestamp}
import org.slf4j.{Logger, LoggerFactory}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.io.dot.{DotEdgeStmt, DotGraph, DotRootGraph, Id, NodeId, graph2DotExport}
import scalax.collection.mutable.Graph
import scalax.collection.{Graph => ImGraph}

import scala.collection.mutable.ListBuffer


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

    require(this.graph.isAcyclic, "The constraints cannot be mutually exclusive")

    visualizeGraph("graph")
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

      val bufferedToPublish = ListBuffer.empty[ValueAndTimestamp[KeyValue[K, V]]]
      successors.toList.tail.foreach(node => {
        if (node.value.buffered && node.diPredecessors.forall(node => node.value.seen)) {
          val buffered = bufferStore(node.value.name).get(link)
          bufferedToPublish.addAll(buffered)
          bufferStore(node.value.name).delete(link)

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
      val buffered = Option(bufferStore(constraintNode.get.value.name).get(link))
        .getOrElse(List.empty[ValueAndTimestamp[KeyValue[K, V]]])
      val newList = buffered.appended(ValueAndTimestamp.make(KeyValue.pair(key, value), context.timestamp()))
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

  private def visualizeGraph(name: String): File = {
    val dotRoot = DotRootGraph(directed = true, id = Some(Id("Constraints")))
    def edgeTransformer(innerEdge: ImGraph[ConstraintNode, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(dotRoot,
        DotEdgeStmt(NodeId(edge.from.value.name), NodeId(edge.to.value.name), Nil))
    }

    Graphviz.fromString(this.graph.toDot(dotRoot, edgeTransformer))
      .scale(2)
      .render(Format.PNG)
      .toFile(new File(s"graphs/$name.png"))
  }

}