package com.github.annterina.stream_constraints

import com.github.annterina.stream_constraints.constraints.Constraint
import com.github.annterina.stream_constraints.graphs.{ConstraintNode, GraphVisualization, WindowLabel}
import com.github.annterina.stream_constraints.stores.PrerequisiteStores
import com.github.annterina.stream_constraints.transformers.{Redirect, StateConstraintTransformer, WindowConstraintTransformer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef.EdgeAssoc
import scalax.collection.edge.Implicits.any2XEdgeAssoc
import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.Graph

import scala.collection.mutable

class ConstrainedKStream[K, V, L](inner: KStream[K, V], builder: StreamsBuilder) {

  def constrain(constraint: Constraint[K, V, L]): ConstrainedKStream[K, V, L] = {

    val storeProvider = PrerequisiteStores(constraint)

    // TODO here we probably only need to add the store for "before"
    val constraintStateStores = constraint.prerequisites
      .foldLeft(mutable.Set.empty[String])((stores, prerequisite) => {
        Seq(prerequisite.before._2, prerequisite.after._2)
          .foreach(name => {
            if (!stores.contains(name)) {
              stores.add(name)
              builder.addStateStore(storeProvider.bufferStore(name))
            }
        })

        stores
      })

    val graphs = storeProvider.graphStore()
    builder.addStateStore(graphs)
    constraintStateStores.add(graphs.name)


    val terminated = storeProvider.terminatedStore()
    builder.addStateStore(terminated)
    constraintStateStores.add(terminated.name)

    val constraintGraph = windowConstraintGraph(constraint)
    val graphTemplate = prerequisiteGraph(constraint)
    GraphVisualization.visualize(constraintGraph, graphTemplate)

    val windowStateStores = constraint.windowConstraints
      .foldLeft(mutable.Set.empty[String])((stores, windowConstraint) => {
        val name = windowConstraint.before._2
        if (!stores.contains(name)) {
          stores.add(name)
          builder.addStateStore(storeProvider.windowedStore(name, windowConstraint.window))
        }
        stores
      })

    //if (constraint.windowConstraints.nonEmpty)
    val windowStream = inner
      .transform(() => new WindowConstraintTransformer(constraint, constraintGraph), windowStateStores.toList:_*)

    val windowedConstrainedStream = redirect(windowStream, constraint)

    val prerequisiteStream = windowedConstrainedStream
      .transform(() => new StateConstraintTransformer(constraint, graphTemplate), constraintStateStores.toList:_*)

    val prerequisiteConstrainedStream = redirect(prerequisiteStream, constraint)

    new ConstrainedKStream(prerequisiteConstrainedStream, builder)
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): ConstrainedKStream[KR, VR, L] =
    new ConstrainedKStream(inner.map[KR, VR](mapper), builder)

  def filter(predicate: (K, V) => Boolean): ConstrainedKStream[K, V, L] =
    new ConstrainedKStream(inner.filter(predicate), builder)

  def to(topic: String)(implicit produced: Produced[K, V]): Unit =
    inner.to(topic)(produced)

  private def windowConstraintGraph(constraint: Constraint[K, V, L]): Graph[ConstraintNode, LDiEdge] = {
    val graph = Graph.empty[ConstraintNode, LDiEdge]

    constraint.windowConstraints.foreach(c => {
      val (before, after) = (ConstraintNode(c.before._2), ConstraintNode(c.after._2))
      graph.add((before ~+> after)(WindowLabel(c.window, c.action)))
    })

    graph
  }

  private def prerequisiteGraph(constraint: Constraint[K, V, L]): Graph[ConstraintNode, DiEdge] = {
    val graph = Graph.empty[ConstraintNode, DiEdge]

    constraint.prerequisites.foreach(c => {
      val (before, after) = (ConstraintNode(c.before._2), ConstraintNode(c.after._2))
      graph.add(before ~> after)
    })

    constraint.terminals.foreach(c => {
      val node = graph.nodes.find(n => n.value.name == c.terminal._2)
      if (node.isDefined)
        node.get.value.terminal = true
      else {
        graph.add(ConstraintNode(c.terminal._2, terminal = true))
      }
    })

    graph
  }

  private def redirect(stream: KStream[Redirect[K], V], constraint: Constraint[K, V, L]): KStream[K, V] = {
    val streamBranches = stream.branch(
      (key, _) => key.redirect,
      (_, _) => true
    )

    if (constraint.redirectTopic.isDefined) {
      streamBranches(0)
        .selectKey((redirectKey, _) => redirectKey.key)
        .to(constraint.redirectTopic.get)(Produced.`with`(constraint.keySerde, constraint.valueSerde))
    }

    streamBranches(1).selectKey((redirectKey, _) => redirectKey.key)
  }

}