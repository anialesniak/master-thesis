package com.github.annterina.stream_constraints.constraints

import org.apache.kafka.common.serialization.Serde

trait Constraint[K, V] {

  var link: Either[K => Any, V => Any] = _
  var linkSerde: Serde[_ >: Any] = _

  var keySerde: Serde[K] = _
  var valueSerde: Serde[V] = _

  def withKeyLink(f: K => Any, serde: Serde[_ >: Any]): Constraint[K, V] = {
    link = Left(f)
    linkSerde = serde
    this
  }

  def withValueLink(f: V => Any, serde: Serde[_ >: Any]): Constraint[K, V] = {
    link = Right(f)
    linkSerde = serde
    this
  }

  def withKeySerde(keySerdes: Serde[K]): Constraint[K, V] = {
    keySerde = keySerdes
    this
  }

  def withValueSerde(valueSerdes: Serde[V]): Constraint[K, V] = {
    valueSerde = valueSerdes
    this
  }
}

case class PrerequisiteConstraint[K, V](atLeastOnce: V => Boolean, before: V => Boolean) extends Constraint[K, V] {

}