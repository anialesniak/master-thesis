package com.github.annterina.stream_constraints

import java.io.File

import com.github.annterina.stream_constraints.constraints.{Constraint, MultiConstraint}
import com.github.annterina.stream_constraints.serdes.{GraphSerde, KeyValueListSerde, KeyValueSerde}
import com.github.annterina.stream_constraints.transformers.graphs.ConstraintNode
import com.github.annterina.stream_constraints.transformers.{MultiConstraintTransformer, Redirect, TerminalTransformer}
import guru.nidi.graphviz.engine.{Format, Graphviz}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores, ValueAndTimestamp}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.io.dot.{DotEdgeStmt, DotGraph, DotRootGraph, Id, NodeId, graph2DotExport}
import scalax.collection.mutable.Graph
import scalax.collection.{Graph => ImGraph}

import scala.collection.mutable

class ConstrainedKStream[K, V, L](inner: KStream[K, V], builder: StreamsBuilder) {

  def constrain(constraint: Constraint[K, V, L]): ConstrainedKStream[K, V, L] = {
    constraint match {
      case multiConstraint: MultiConstraint[K, V, L] =>
        val graphs = graphStore(multiConstraint)
        builder.addStateStore(graphs)

        val terminated = terminatedStore(multiConstraint)
        builder.addStateStore(terminated)

        val names = multiConstraint.prerequisites
          .foldLeft(mutable.Set.empty[String])((names, prerequisite) =>  {
            val beforeName = prerequisite.before._2
            if (!names.contains(beforeName)) {
              names.add(beforeName)
              builder.addStateStore(bufferStore(multiConstraint, beforeName))
            }

            val afterName = prerequisite.after._2
            if (!names.contains(afterName)) {
              names.add(afterName)
              builder.addStateStore(bufferStore(multiConstraint, afterName))
            }
            names
          })

        names.add(graphs.name())
        names.add(terminated.name())

        multiConstraint.terminals.map(t => t.terminal._1)

        val graphTemplate = createGraph(multiConstraint)

        val branches: Array[KStream[Redirect[K], V]] = inner
          .transform(() => new MultiConstraintTransformer(multiConstraint, graphTemplate), names.toList:_*)
          .branch(
            (key, _) => key.redirect,
            (_, _) => true
          )

        if (multiConstraint.redirectTopic.isDefined) {
          branches(0)
            .selectKey((redirectKey, _) => redirectKey.key)
            .to(multiConstraint.redirectTopic.get)(Produced.`with`(constraint.keySerde, constraint.valueSerde))
        }

        val constrained = branches(1).selectKey((redirectKey, _) => redirectKey.key)
        new ConstrainedKStream(constrained, builder)
    }
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): ConstrainedKStream[KR, VR, L] =
    new ConstrainedKStream(inner.map[KR, VR](mapper), builder)

  def filter(predicate: (K, V) => Boolean): ConstrainedKStream[K, V, L] =
    new ConstrainedKStream(inner.filter(predicate), builder)

  def to(topic: String)(implicit produced: Produced[K, V]): Unit =
    inner.to(topic)(produced)


  private def graphStore(constraint: Constraint[K, V, L]): StoreBuilder[KeyValueStore[L, Graph[ConstraintNode, DiEdge]]] = {
    val name = "Graph"
    val graphStoreSupplier = Stores.persistentKeyValueStore(name)
    Stores.keyValueStoreBuilder(graphStoreSupplier, constraint.linkSerde, GraphSerde)
  }

  private def bufferStore(constraint: Constraint[K, V, L], name: String): StoreBuilder[KeyValueStore[L, List[ValueAndTimestamp[KeyValue[K, V]]]]] = {
    val storeSupplier = Stores.persistentKeyValueStore(name)
    val keyValueSerde = new KeyValueSerde[K, V](constraint.keySerde, constraint.valueSerde)
    Stores.keyValueStoreBuilder(storeSupplier, constraint.linkSerde, new KeyValueListSerde[K, V](keyValueSerde))
  }

  private def terminatedStore(constraint: Constraint[K, V, L]): StoreBuilder[KeyValueStore[L, Long]] = {
    val name = "Terminated"
    val storeSupplier = Stores.persistentKeyValueStore(name)
    Stores.keyValueStoreBuilder(storeSupplier, constraint.linkSerde, Serdes.longSerde)
  }

  private def createGraph(constraint: MultiConstraint[K, V, L]): Graph[ConstraintNode, DiEdge] = {
    val graph = Graph.empty[ConstraintNode, DiEdge]

    constraint.prerequisites.foreach(c => {
      val (before, after) = (ConstraintNode(c.before._2, seen = false, buffered = false, "STANDARD"),
        ConstraintNode(c.after._2, seen = false, buffered = false, "STANDARD"))
      graph.addEdge(before, after)(DiEdge)
    })

    constraint.terminals.foreach(c => {
      val node = graph.nodes.find(n => n.value.name == c.terminal._2)
      if (node.isDefined)
        node.get.value.nodeType = "TERMINAL"
      else {
        graph.add(ConstraintNode(c.terminal._2, seen = false, buffered = false, "TERMINAL"))
      }
    })

    visualizeGraph(graph)

    graph
  }

  private def visualizeGraph(graph: Graph[ConstraintNode, DiEdge]): File = {
    val dotRoot = DotRootGraph(directed = true, id = Some(Id("Constraints")))
    def edgeTransformer(innerEdge: ImGraph[ConstraintNode, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(dotRoot, DotEdgeStmt(NodeId(edge.from.value.name), NodeId(edge.to.value.name), Nil))
    }

    Graphviz.fromString(graph.toDot(dotRoot, edgeTransformer))
      .scale(2)
      .render(Format.PNG)
      .toFile(new File(s"graphs/constraints.png"))
  }
}