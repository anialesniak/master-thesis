package com.github.annterina.stream_constraints.transformers.graphs

case class ConstraintNode(name: String, var seen: Boolean, var buffered: Boolean) {

  override def hashCode(): Int =
    name.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case l: ConstraintNode => l.name == this.name
      case _ => super.equals(obj)
    }
  }
}
