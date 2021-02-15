package com.github.annterina.stream_constraints.constraints

class PrerequisiteConstraintBuilder[K, V](atLeastOnce: V => Boolean) {

  def before(f: V => Boolean): ConditionConstraintBuilder[K, V] = {
    val constraint = new PrerequisiteConstraint[K, V](atLeastOnce, f)
    new ConditionConstraintBuilder[K, V](constraint)
  }

}
