package com.github.annterina.stream_constraints.constraints.window

import java.time.Duration

class WindowConstraintBuilder[K, V] {

  def before(f: ((K, V) => Boolean, String)): AfterStep[K, V] = {
    new AfterStep[K, V](f)
  }

  final class AfterStep[K, V](before: ((K, V) => Boolean, String)) {
     def after(f: ((K, V) => Boolean, String)): WindowStep[K, V] = {
      new WindowStep[K, V](before, f)
     }
  }

  final class WindowStep[K, V](before: ((K, V) => Boolean, String),
                               after: ((K, V) => Boolean, String)) {
    def window(duration: Duration): ActionStep[K, V] = {
      new ActionStep[K, V](before, after, duration)
    }
  }

  final class ActionStep[K, V](before: ((K, V) => Boolean, String),
                                 after: ((K, V) => Boolean, String),
                                 duration: Duration) {
    def swap: WindowConstraint[K, V] = {
      new WindowConstraint[K, V](before, after, duration, Swap)
    }

    def dropBefore: WindowConstraint[K, V] = {
      new WindowConstraint[K, V](before, after, duration, DropBefore)
    }

    def dropAfter: WindowConstraint[K, V] = {
      new WindowConstraint[K, V](before, after, duration, DropAfter)
    }
  }
}
