package com.github.annterina.stream_constraints.serdes

import com.github.annterina.stream_constraints.graphs.{ConstraintLabel, ConstraintNode}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scalax.collection.edge.LDiEdge
import scalax.collection.io.json.{JsonGraph, JsonGraphCoreCompanion}
import scalax.collection.io.json.descriptor.{Descriptor, LEdgeDescriptor, NodeDescriptor}
import scalax.collection.io.json.serializer.LEdgeSerializer
import scalax.collection.mutable.Graph

object GraphSerde extends Serde[Graph[ConstraintNode, LDiEdge]] {

  override def serializer(): Serializer[Graph[ConstraintNode, LDiEdge]] =
    (_: String, data: Graph[ConstraintNode, LDiEdge]) => {
      data.toJson(descriptor).getBytes
    }

  override def deserializer(): Deserializer[Graph[ConstraintNode, LDiEdge]] =
    (_: String, data: Array[Byte]) => {
      Graph.fromJson(new String(data), descriptor)
    }

  private def descriptor: Descriptor[ConstraintNode] = {
    val nodeDescriptor = new NodeDescriptor[ConstraintNode](typeId = "Nodes")  {
      def id(node: Any): String = node match {
        case ConstraintNode(name, _, _, _) => name
      }
    }
    val serializer = new LEdgeSerializer[ConstraintLabel](new ConstraintLabelSerde)
    val edgeDescriptor = new LEdgeDescriptor[ConstraintNode, LDiEdge, LDiEdge.type, ConstraintLabel](
      LDiEdge, new ConstraintLabel, Some(serializer), List(classOf[ConstraintLabel]))
    new Descriptor[ConstraintNode](nodeDescriptor, edgeDescriptor)
  }
}
