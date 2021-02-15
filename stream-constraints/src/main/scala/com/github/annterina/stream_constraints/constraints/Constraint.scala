package com.github.annterina.stream_constraints.constraints

trait Constraint[K, V] {

  var keyLink: K => Any = _
  var valueLink: V => Any = _

  def withKeyLink(f: K => Any): Constraint[K, V] = {
    keyLink = f
    this
  }

  def withValueLink(f: V => Any): Constraint[K, V] = {
    valueLink = f
    this
  }
}

case class PrerequisiteConstraint[K, V](atLeastOnce: V => Boolean, before: V => Boolean) extends Constraint[K, V] {

}