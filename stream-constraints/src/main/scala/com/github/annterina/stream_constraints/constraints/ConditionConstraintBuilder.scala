package com.github.annterina.stream_constraints.constraints

import org.apache.kafka.common.serialization.Serde

class ConditionConstraintBuilder[K, V](constraint: Constraint[K, V]) {

  def keyLink(f: K => Any)(implicit serde: Serde[_ >: Any]): ConditionConstraintBuilder[K, V] = {
    new ConditionConstraintBuilder[K, V](constraint.withKeyLink(f, serde))
  }

  def valueLink(f: V => Any)(implicit serde: Serde[_ >: Any]): ConditionConstraintBuilder[K, V] = {
    new ConditionConstraintBuilder[K, V](constraint.withValueLink(f, serde))
  }

  // TODO additional filtering
  def whereKey(f: K => Any) = {}

  def whereValue(f: V => Any) = {}

  def build(keySerde: Serde[K], valueSerde: Serde[V]): Constraint[K, V] = {
    constraint.withKeySerde(keySerde).withValueSerde(valueSerde)
  }

}

