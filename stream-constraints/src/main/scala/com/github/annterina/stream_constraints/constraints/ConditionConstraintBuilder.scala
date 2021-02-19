package com.github.annterina.stream_constraints.constraints

import org.apache.kafka.common.serialization.Serde

class ConditionConstraintBuilder[K, V, L](constraint: Constraint[K, V, L]) {

  def keyLink(f: K => L)(implicit serde: Serde[L]): ConditionConstraintBuilder[K, V, L] = {
    new ConditionConstraintBuilder[K, V, L](constraint.withKeyLink(f, serde))
  }

  def valueLink(f: V => L)(implicit serde: Serde[L]): ConditionConstraintBuilder[K, V, L] = {
    new ConditionConstraintBuilder[K, V, L](constraint.withValueLink(f, serde))
  }

  // TODO additional filtering
  def whereKey(f: K => Any) = {}

  def whereValue(f: V => Any) = {}

  def build(keySerde: Serde[K], valueSerde: Serde[V]): Constraint[K, V, L] = {
    constraint.withKeySerde(keySerde).withValueSerde(valueSerde)
  }

}

