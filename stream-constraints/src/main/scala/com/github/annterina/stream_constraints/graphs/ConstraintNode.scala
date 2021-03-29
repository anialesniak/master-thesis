package com.github.annterina.stream_constraints.graphs

case class ConstraintNode(name: String, var seen: Boolean = false,
                          var buffered: Boolean = false, var terminal: Boolean = false) {

  override def hashCode(): Int =
    name.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case l: ConstraintNode => l.name == this.name
      case _ => super.equals(obj)
    }
  }
}