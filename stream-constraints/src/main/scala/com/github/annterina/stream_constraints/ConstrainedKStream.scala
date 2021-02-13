package com.github.annterina.stream_constraints

import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.kstream.{KStream => KStreamJ}

class ConstrainedKStream[K, V](inner: KStreamJ[K,V]) extends KStream[K, V](inner) {


}
