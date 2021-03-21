package com.github.annterina.stream_constraints.transformers.graphs

import com.github.annterina.stream_constraints.transformers.graphs.NodeTypes.NodeType

case class ConstraintNode(name: String, var seen: Boolean, var buffered: Boolean, var nodeType: String) {

  override def hashCode(): Int =
    name.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case l: ConstraintNode => l.name == this.name
      case _ => super.equals(obj)
    }
  }
}

object NodeTypes extends Enumeration {
  type NodeType = Value
  val Standard, Terminal = Value
}