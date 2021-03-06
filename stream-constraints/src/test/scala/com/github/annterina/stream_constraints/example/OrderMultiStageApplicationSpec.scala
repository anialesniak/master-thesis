package com.github.annterina.stream_constraints.example

import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderMultiStageApplicationSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, OrderEvent] = _
  private var outputTopic: TestOutputTopic[String, OrderEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

    val constraint = new ConstraintBuilder[String, OrderEvent, Integer]
      .atLeastOnce(e => e.action == "CREATED")
      .before(e => e.action == "UPDATED")
      .valueLink(e => e.key)(Serdes.Integer)
      .build(Serdes.String, orderEventSerde)

    val deleteConstraint = new ConstraintBuilder[String, OrderEvent, Integer]
      .atLeastOnce(e => e.action == "CREATED")
      .before(e => e.action == "DELETED")
      .valueLink(e => e.key)(Serdes.Integer)
      .build(Serdes.String, orderEventSerde)

    val updateDeleteConstraint = new ConstraintBuilder[String, OrderEvent, Integer]
      .atLeastOnce(e => e.action == "UPDATED")
      .before(e => e.action == "DELETED")
      .valueLink(e => e.key)(Serdes.Integer)
      .build(Serdes.String, orderEventSerde)

    val builder = new CStreamsBuilder()

    builder
      .stream("orders")(Consumed.`with`(Serdes.String, orderEventSerde))
      .constrain(constraint)
      .constrain(deleteConstraint)
      //.constrain(updateDeleteConstraint)
      .to("orders-output-topic")(Produced.`with`(Serdes.String, orderEventSerde))

    testDriver = new TopologyTestDriver(builder.build(), config)

    inputTopic = testDriver.createInputTopic(
      "orders",
      Serdes.String.serializer(),
      OrderEventSerde.serializer()
    )

    outputTopic = testDriver.createOutputTopic(
      "orders-output-topic",
      Serdes.String.deserializer(),
      OrderEventSerde.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Order Application") {

    it("should emit the prerequisite event") {
      inputTopic.pipeInput("123", OrderEvent(1, "CREATED"))

      val output = outputTopic.readKeyValue()

      assert(output.key == "123")
      assert(output.value.key == 1)
      assert(output.value.action == "CREATED")
    }

    it("should buffer an event when the prerequisite was not processed") {
      inputTopic.pipeInput("123", OrderEvent(1, "UPDATED"))
      inputTopic.pipeInput("123", OrderEvent(1, "DELETED"))

      assert(outputTopic.isEmpty)
    }

    it("should buffer an event when the prerequisite was not processed and publish not related event") {
      inputTopic.pipeInput("456", OrderEvent(1, "DELETED"))
      inputTopic.pipeInput("123", OrderEvent(2, "CREATED"))

      val output = outputTopic.readKeyValue()

      assert(output.key == "123")
      assert(output.value.key == 2)
      assert(output.value.action == "CREATED")
    }

    it("should publish both events after receiving the prerequisite") {
      inputTopic.pipeInput("123", OrderEvent(1, "DELETED"))
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"))
      inputTopic.pipeInput("789", OrderEvent(1, "CREATED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "789")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "123")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "DELETED")
    }
  }

}
