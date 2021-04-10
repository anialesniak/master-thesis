package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import com.github.annterina.stream_constraints.constraints.window.{DropAfter, DropBefore, Swap}
import com.github.annterina.stream_constraints.graphs.{ConstraintNode, WindowLabel}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.WindowStore
import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.Graph

class WindowConstraintTransformer[K, V, L](constraint: Constraint[K, V, L], graph: Graph[ConstraintNode, LDiEdge])
  extends Transformer[K, V, KeyValue[Redirect[K], V]] {

  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context

    constraint.windowConstraints.foreach(constraint => {
      val store = context.getStateStore[WindowStore[L, KeyValue[K, V]]](constraint.before._2)

      this.context.schedule(constraint.window.dividedBy(2), PunctuationType.STREAM_TIME, new Punctuator {
        override def punctuate(timestamp: Long): Unit = {
          val iter = store.fetchAll(0, timestamp - constraint.window.toMillis)
          while (iter.hasNext) {
            val entry = iter.next
            store.put(entry.key.key(), null, entry.key.window().start())
            context.forward(Redirect(entry.value.key, redirect = false), entry.value.value)
          }
          iter.close()
        }
      })
    })
  }

  override def transform(key: K, value: V): KeyValue[Redirect[K], V] = {
    val link = constraint.link.apply(key, value)

    if (!windowConstraintsApplicable(key, value)) {
      context.forward(Redirect(key, redirect = false), value)
      return null
    }

    val constraintNode = graph.nodes.find(node => constraint.names(node.value.name).apply(key, value))
    val before = constraintNode.get.diPredecessors

    if (before.isEmpty) {
      val store = context.getStateStore[WindowStore[L, KeyValue[K, V]]](constraintNode.get.value.name)
      store.put(link, KeyValue.pair(key, value), context.timestamp())
    } else {
      before.foreach(nodeBefore => {
        val store = context.getStateStore[WindowStore[L, KeyValue[K, V]]](nodeBefore.value.name)

        val (before, after) = (nodeBefore.value, constraintNode.get.value)
        val edge = graph.edges.find(e => e.from == before && e.to == after)
        val label = edge.get.label.asInstanceOf[WindowLabel]
        val now = context.timestamp()

        val buffered = store.fetch(link, now - label.window.toMillis, now)

        if (!buffered.hasNext) {
          context.forward(Redirect(key, redirect = false), value)
          return null
        }

        label.action match {
          case Swap =>
            context.forward(Redirect(key, redirect = false), value)
            while (!constraint.withFullWindows && buffered.hasNext) {
              val entry = buffered.next()
              store.put(link, null, entry.key)
              context.forward(Redirect(entry.value.key, redirect = false), entry.value.value)
            }
          case DropBefore =>
            while (!constraint.withFullWindows && buffered.hasNext) {
              val entry = buffered.next()
              store.put(link, null, entry.key)
            }
            context.forward(Redirect(key, redirect = false), value)
          case DropAfter =>
            while (!constraint.withFullWindows && buffered.hasNext) {
              val entry = buffered.next
                store.put(link, null, entry.key)
                context.forward(Redirect(entry.value.key, redirect = false), entry.value.value)
            }
        }

        buffered.close()
      })
    }

    null
  }

  override def close(): Unit = {}

  private def windowConstraintsApplicable(key: K, value: V): Boolean =
    constraint.windowConstraints
      .exists(constraint => constraint.before._1(key, value) || constraint.after._1(key, value))
}
