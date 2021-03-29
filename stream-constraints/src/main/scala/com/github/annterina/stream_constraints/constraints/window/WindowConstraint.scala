package com.github.annterina.stream_constraints.constraints.window

import java.time.Duration

case class WindowConstraint[K, V](before: ((K, V) => Boolean, String),
                                  after: ((K, V) => Boolean, String),
                                  window: Duration,
                                  action: WindowAction) {}

sealed trait WindowAction

case object Swap extends WindowAction {
  override def toString: String = "Swap"
}

case object DropBefore extends WindowAction {
  override def toString: String = "Drop former"
}

case object DropAfter extends WindowAction {
  override def toString: String = "Drop latter"

}
