package com.github.annterina.stream_constraints.serdes

import java.time.Duration

import com.github.annterina.stream_constraints.constraints.window.{DropAfter, DropBefore, Swap, WindowAction}
import com.github.annterina.stream_constraints.graphs.{ConstraintLabel, GeneralLabel, WindowLabel}
import net.liftweb.json.{Formats, JField, JObject, JString, JValue, Serializer, TypeInfo}

class ConstraintLabelSerde extends Serializer[ConstraintLabel] {

  val LabelClass: Class[ConstraintLabel] = classOf[ConstraintLabel]

  override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), ConstraintLabel] = {
    case (TypeInfo(LabelClass, _), json) => json match {
      case JObject(JField("window", JString(window)) :: JField("action", JString(action)) :: Nil) =>
       WindowLabel(Duration.parse(window), actionObject(action))
      case JObject(_) =>
        GeneralLabel()
    }
  }

  override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case WindowLabel(window, action) =>
      JObject(JField("window", JString(window.toString)) :: JField("action", JString(action.toString)) :: Nil)
    case GeneralLabel => JObject()
  }

  private def actionObject(action: String): WindowAction = {
    action match {
      case "Swap" => Swap
      case "Drop latter" => DropAfter
      case "Drop former" => DropBefore
    }
  }
}
