package com.github.annterina.stream_constraints.constraints

class PrerequisiteConstraintBuilder[K, V, L](atLeastOnce: V => Boolean) {

  def before(f: V => Boolean): ConditionConstraintBuilder[K, V, L] = {
    val constraint = new PrerequisiteConstraint[K, V, L](atLeastOnce, f)
    new ConditionConstraintBuilder[K, V, L](constraint)
  }

}
