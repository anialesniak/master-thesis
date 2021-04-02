package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import com.github.annterina.stream_constraints.graphs.ConstraintNode
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.Graph

class WindowConstraintTransformer[K, V, L](constraint: Constraint[K, V, L], graph: Graph[ConstraintNode, LDiEdge])
  extends Transformer[K, V, KeyValue[Redirect[K], V]] {

  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(key: K, value: V): KeyValue[Redirect[K], V] = {

    if (!windowConstraintsApplicable(key, value)) {
      context.forward(Redirect(key, redirect = false), value)
      return null
    }

    context.forward(Redirect(key, redirect = false), value)
    null
  }

  override def close(): Unit = {}

  private def windowConstraintsApplicable(key: K, value: V): Boolean =
    constraint.windowConstraints
      .exists(constraint => constraint.before._1(key, value) || constraint.after._1(key, value))
}
