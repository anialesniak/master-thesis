package com.github.annterina.stream_constraints.graphs

import java.time.Duration

import com.github.annterina.stream_constraints.constraints.window.WindowAction

sealed class ConstraintLabel

case class WindowLabel(window: Duration, action: WindowAction) extends ConstraintLabel {}
case class GeneralLabel() extends ConstraintLabel {}
