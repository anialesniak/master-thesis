package com.github.annterina.stream_constraints.constraints

class ConditionConstraintBuilder[K, V](constraint: Constraint[K, V]) {

  def keyLink(f: K => Any): ConditionConstraintBuilder[K, V] = {
    new ConditionConstraintBuilder[K, V](constraint.withKeyLink(f))
  }

  def valueLink(f: V => Any): ConditionConstraintBuilder[K, V] = {
    new ConditionConstraintBuilder[K, V](constraint.withValueLink(f))
  }

  // TODO additional filtering
  def whereKey(f: K => Any) = {}

  def whereValue(f: V => Any) = {}

  def build(): Constraint[K, V] = {
    constraint
  }

}

