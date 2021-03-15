package com.github.annterina.stream_constraints.constraints

import org.apache.kafka.common.serialization.Serde

import scala.collection.mutable

class MultiConstraintBuilder[K, V, L] {

  private val constraintSet: mutable.Set[Prerequisite[K, V]] = mutable.Set.empty
  private val constraintNames: mutable.Map[String, (K, V) => Boolean] = mutable.Map.empty

  def prerequisite(before: ((K, V) => Boolean, String), after: ((K, V) => Boolean, String)): MultiConstraintBuilder[K, V, L] = {
    constraintSet.add(new Prerequisite[K, V](before, after))
    constraintNames += before.swap
    constraintNames += after.swap
    this
  }

  def link(f: (K, V) => L)(implicit serde: Serde[L]): ConditionConstraintBuilder[K, V, L] = {
    val constraint = MultiPrerequisiteConstraint[K, V, L](constraintSet.toSet, constraintNames.toMap).withLink(f, serde)
    new ConditionConstraintBuilder[K, V, L](constraint)
  }

}
