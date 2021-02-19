package com.github.annterina.stream_constraints.constraints

class ConstraintBuilder[K, V, L] {

  def atLeastOnce(f: V => Boolean): PrerequisiteConstraintBuilder[K, V, L] = {
     new PrerequisiteConstraintBuilder[K, V, L](f)
  }

}
