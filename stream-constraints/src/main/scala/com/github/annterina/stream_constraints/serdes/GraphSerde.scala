package com.github.annterina.stream_constraints.serdes

import com.github.annterina.stream_constraints.transformers.graphs.ConstraintNode
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.io.json.{JsonGraph, JsonGraphCoreCompanion}
import scalax.collection.io.json.descriptor.predefined.Di
import scalax.collection.io.json.descriptor.{Descriptor, NodeDescriptor}
import scalax.collection.mutable.Graph

object GraphSerde extends Serde[Graph[ConstraintNode, DiEdge]] {

  override def serializer(): Serializer[Graph[ConstraintNode, DiEdge]] =
    (_: String, data: Graph[ConstraintNode, DiEdge]) => {
      data.toJson(descriptor).getBytes
    }

  override def deserializer(): Deserializer[Graph[ConstraintNode, DiEdge]] =
    (_: String, data: Array[Byte]) => {
      Graph.fromJson(new String(data), descriptor)
    }

  private def descriptor: Descriptor[ConstraintNode] = {
    val nodeDescriptor = new NodeDescriptor[ConstraintNode](typeId = "Nodes")  {
      def id(node: Any): String = node match {
        case ConstraintNode(name, _, _) => name
      }
    }
    new Descriptor[ConstraintNode](nodeDescriptor, Di.descriptor[ConstraintNode]())
  }
}
