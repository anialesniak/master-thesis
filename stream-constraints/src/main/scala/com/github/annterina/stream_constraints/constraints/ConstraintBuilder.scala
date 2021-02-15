package com.github.annterina.stream_constraints.constraints

class ConstraintBuilder[K, V] {

  def atLeastOnce(f: V => Boolean): PrerequisiteConstraintBuilder[K, V] = {
     new PrerequisiteConstraintBuilder[K, V](f)
  }

}
