package com.github.annterina.stream_constraints.constraints.window

import java.time.Duration

case class WindowConstraint[K, V](before: ((K, V) => Boolean, String),
                                  after: ((K, V) => Boolean, String),
                                  window: Duration,
                                  action: WindowAction) {}

sealed trait WindowAction

case object Swap extends WindowAction
case object DropBefore extends WindowAction
case object DropAfter extends WindowAction
