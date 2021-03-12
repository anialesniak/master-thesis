package com.github.annterina.stream_constraints.constraints

class ConstraintBuilder[K, V, L] {

  //TODO here too
  def prerequisite(atLeastOnce: V => Boolean, before: V => Boolean): ConditionConstraintBuilder[K, V, L] = {
    val constraint = new PrerequisiteConstraint[K, V, L](atLeastOnce, before)
    new ConditionConstraintBuilder[K, V, L](constraint)
  }

}
