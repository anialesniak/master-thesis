package com.github.annterina.stream_constraints.constraints

import org.apache.kafka.common.serialization.Serde

import scala.collection.mutable

class ConstraintBuilder[K, V, L] {

  private val prerequisites: mutable.Set[Prerequisite[K, V]] = mutable.Set.empty
  private val terminals: mutable.Set[Terminal[K, V]] = mutable.Set.empty
  private val constraintNames: mutable.Map[String, (K, V) => Boolean] = mutable.Map.empty
  private var redirectTopic: Option[String] = None

  def prerequisite(before: ((K, V) => Boolean, String), after: ((K, V) => Boolean, String)): ConstraintBuilder[K, V, L] = {
    prerequisites.add(new Prerequisite[K, V](before, after))
    constraintNames += before.swap
    constraintNames += after.swap
    this
  }

  def terminal(terminal: ((K, V) => Boolean, String)): ConstraintBuilder[K, V, L] = {
    terminals.add(new Terminal[K, V](terminal))
    constraintNames += terminal.swap
    this
  }

  def redirect(topic: String): ConstraintBuilder[K, V, L] = {
    redirectTopic = Some(topic)
    this
  }

  def link(f: (K, V) => L)(implicit serde: Serde[L]): ConditionConstraintBuilder[K, V, L] = {
    val constraint = MultiConstraint[K, V, L](prerequisites.toSet, terminals.toSet, constraintNames.toMap, redirectTopic)
      .withLink(f, serde)
    new ConditionConstraintBuilder[K, V, L](constraint)
  }

}
