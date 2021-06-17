package com.github.annterina.stream_constraints.transformers

import com.github.annterina.stream_constraints.constraints.Constraint
import com.github.annterina.stream_constraints.constraints.window.{DropAfter, DropBefore, Swap}
import com.github.annterina.stream_constraints.graphs.{ConstraintNode, WindowLabel}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore, WindowStoreIterator}
import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.Graph

import scala.collection.mutable.ListBuffer

class WindowConstraintTransformer[K, V, L](constraint: Constraint[K, V, L], graph: Graph[ConstraintNode, LDiEdge])
  extends Transformer[K, V, KeyValue[Redirect[K], V]] {

  type KeyValueList = List[KeyValue[K, V]]

  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context

    require(noMutuallyExclusivePredecessors(graph),
      "Window constraints with the same node cannot have mutually exclusive actions.")

    require(noMutuallyExclusiveSuccessors(graph),
      "Window constraints with the same node cannot have mutually exclusive actions.")

    constraint.windowConstraints.foreach(constraint => {
      val storeName = constraint.before._2 ++ "-window"
      val store = context.getStateStore[WindowStore[L, KeyValueList]](storeName)

      this.context.schedule(constraint.window.dividedBy(2), PunctuationType.STREAM_TIME, new Punctuator {
        override def punctuate(timestamp: Long): Unit = {
          val iter = store.fetchAll(0, timestamp - constraint.window.toMillis)
          while (iter.hasNext) {
            val entry = iter.next
            store.put(entry.key.key, null, entry.key.window.start)
            entry.value.foreach(keyValue => context.forward(Redirect(keyValue.key, redirect = false), keyValue.value))
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
      val storeName = constraintNode.get.value.name ++ "-window"
      val store = context.getStateStore[WindowStore[L, KeyValueList]](storeName)
      store.put(link, List(KeyValue.pair(key, value)), context.timestamp())
      return null
    }

    val allBeforeToPublish = ListBuffer.empty[ValueAndTimestamp[KeyValue[K, V]]]
    var currentToPublish: Option[KeyValue[K, V]] = None

    before.foreach(nodeBefore => {
      val storeName = nodeBefore.value.name ++ "-window"
      val beforeStore = context.getStateStore[WindowStore[L, KeyValueList]](storeName)

      val (before, after) = (nodeBefore.value, constraintNode.get.value)
      val edge = graph.edges.find(e => e.from == before && e.to == after)
      val label = edge.get.label.asInstanceOf[WindowLabel]
      val now = context.timestamp()

      val bufferedBeforeIterator = beforeStore.fetch(link, now - label.window.toMillis, now + label.window.toMillis)

      if (!constraintNode.get.hasSuccessors) {
        label.action match {
          case Swap =>
            currentToPublish = Some(KeyValue.pair(key, value))
            publishBufferedBefore(bufferedBeforeIterator, beforeStore, link, allBeforeToPublish)
          case DropBefore =>
            dropBufferedBefore(bufferedBeforeIterator, beforeStore, link)
            currentToPublish = Some(KeyValue.pair(key, value))
          case DropAfter =>
            publishBufferedBefore(bufferedBeforeIterator, beforeStore, link, allBeforeToPublish)
            context.forward(Redirect(key, redirect = true), value)
        }
      } else {
        val storeName = constraintNode.get.value.name ++ "-window"
        val store = context.getStateStore[WindowStore[L, KeyValueList]](storeName)

        label.action match {
          case Swap =>
            val recordList = ListBuffer(KeyValue.pair(key, value))
            while (bufferedBeforeIterator.hasNext) {
              val entry = bufferedBeforeIterator.next
              beforeStore.put(link, null, entry.key)
              recordList.addAll(entry.value)
            }
            store.put(link, recordList.toList, context.timestamp())
          case DropBefore =>
            dropBufferedBefore(bufferedBeforeIterator, beforeStore, link)
            store.put(link, List(KeyValue.pair(key, value)), context.timestamp())
          case DropAfter =>
            val recordList = ListBuffer.empty[KeyValue[K, V]]
            while (bufferedBeforeIterator.hasNext) {
              val entry = bufferedBeforeIterator.next
              beforeStore.put(link, null, entry.key)
              recordList.addAll(entry.value)
              store.put(link, recordList.toList, entry.key)
            }
            context.forward(Redirect(key, redirect = true), value)
        }
      }

      bufferedBeforeIterator.close()
    })

    currentToPublish
      .foreach(keyValue => context.forward(Redirect(keyValue.key, redirect = false), keyValue.value))

    allBeforeToPublish
      .distinct
      .sortBy(_.timestamp)
      .map(_.value)
      .foreach(keyValue => context.forward(Redirect(keyValue.key, redirect = false), keyValue.value))

    null
  }

  override def close(): Unit = {}

  private def windowConstraintsApplicable(key: K, value: V): Boolean =
    constraint.windowConstraints
      .exists(constraint => constraint.before._1(key, value) || constraint.after._1(key, value))

  private def publishBufferedBefore(iterator: WindowStoreIterator[KeyValueList],
                                    store: WindowStore[L, KeyValueList],
                                    link: L,
                                    allBeforeToPublish: ListBuffer[ValueAndTimestamp[KeyValue[K, V]]]): Unit = {
    while (!constraint.withFullWindows && iterator.hasNext) {
      val entry = iterator.next
      store.put(link, null, entry.key)
      allBeforeToPublish.addAll(entry.value.map(keyValue => ValueAndTimestamp.make(keyValue, entry.key)))
    }
  }

  private def dropBufferedBefore(iterator: WindowStoreIterator[KeyValueList],
                                 store: WindowStore[L, KeyValueList],
                                 link: L): Unit = {
    while (!constraint.withFullWindows && iterator.hasNext) {
      val entry = iterator.next
      entry.value.foreach(keyValue => context.forward(Redirect(keyValue.key, redirect = true), keyValue.value))
      store.put(link, null, entry.key)
    }
  }

  private def noMutuallyExclusivePredecessors(graph: Graph[ConstraintNode, LDiEdge]): Boolean = {
    graph.nodes.forall(node => {
      val labels = node.incoming
        .map(edge => edge.label.asInstanceOf[WindowLabel].action)
        .toList
        .distinct

      labels.size <= 1 || labels.equals(List(Swap, DropBefore))
    })
  }

  private def noMutuallyExclusiveSuccessors(graph: Graph[ConstraintNode, LDiEdge]): Boolean = {
    graph.nodes.forall(node => {
      val labels = node.outgoing
        .map(edge => edge.label.asInstanceOf[WindowLabel].action)
        .toList
        .distinct

      labels.size <= 1 || labels.equals(List(Swap, DropAfter))
    })
  }
}
