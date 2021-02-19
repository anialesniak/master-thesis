package com.github.annterina.stream_constraints.constraints

import org.apache.kafka.common.serialization.Serde

trait Constraint[K, V, L] {

  var link: Either[K => L, V => L] = _
  var linkSerde: Serde[L] = _

  var keySerde: Serde[K] = _
  var valueSerde: Serde[V] = _

  def withKeyLink(f: K => L, serde: Serde[L]): Constraint[K, V, L] = {
    link = Left(f)
    linkSerde = serde
    this
  }

  def withValueLink(f: V => L, serde: Serde[L]): Constraint[K, V, L] = {
    link = Right(f)
    linkSerde = serde
    this
  }

  def withKeySerde(keySerdes: Serde[K]): Constraint[K, V, L] = {
    keySerde = keySerdes
    this
  }

  def withValueSerde(valueSerdes: Serde[V]): Constraint[K, V, L] = {
    valueSerde = valueSerdes
    this
  }
}

case class PrerequisiteConstraint[K, V, L](atLeastOnce: V => Boolean, before: V => Boolean) extends Constraint[K, V] {

}