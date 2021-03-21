package com.github.annterina.stream_constraints.example

import java.time.Instant
import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderTerminalApplicationSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, OrderEvent] = _
  private var outputTopic: TestOutputTopic[String, OrderEvent] = _
  private var redirectTopic: TestOutputTopic[String, OrderEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

    val constraints = new ConstraintBuilder[String, OrderEvent, Integer]
      .prerequisite(((_, e) => e.action == "CREATED", "Order Created"), ((_, e) => e.action == "DELETED", "Order Deleted"))
      .terminal(((_, order) => order.action == "DELETED", "Order Deleted"))
      .redirect("orders-redirect-topic")
      .link((_, e) => e.key)(Serdes.Integer)
      .build(Serdes.String, orderEventSerde)

    val builder = new CStreamsBuilder()

    builder
      .stream("orders")(Consumed.`with`(Serdes.String, orderEventSerde))
      .constrain(constraints)
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

    redirectTopic = testDriver.createOutputTopic(
      "orders-redirect-topic",
      Serdes.String.deserializer(),
      OrderEventSerde.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Order Terminal Application") {

    it("should emit the prerequisite event") {
      inputTopic.pipeInput("123", OrderEvent(1, "CREATED"))

      val output = outputTopic.readKeyValue()

      assert(output.key == "123")
      assert(output.value.key == 1)
      assert(output.value.action == "CREATED")
    }

    it("should publish prerequisite event and terminal event") {
      inputTopic.pipeInput("123", OrderEvent(1, "CREATED"))
      inputTopic.pipeInput("456", OrderEvent(1, "DELETED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "DELETED")
    }

    it("should redirect event after terminal event") {
      inputTopic.pipeInput("123", OrderEvent(1, "CREATED"))
      inputTopic.pipeInput("456", OrderEvent(1, "DELETED"))
      inputTopic.pipeInput("789", OrderEvent(1, "DELETED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "DELETED")

      assert(outputTopic.isEmpty)

      val redirectOutput = redirectTopic.readKeyValue()

      assert(redirectOutput.key == "789")
      assert(redirectOutput.value.key == 1)
      assert(redirectOutput.value.action == "DELETED")
    }

    it("should redirect buffered terminal event") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")

      inputTopic.pipeInput("456", OrderEvent(1, "DELETED"), timestamp)
      inputTopic.pipeInput("789", OrderEvent(1, "DELETED"), timestamp.plusSeconds(30))
      inputTopic.pipeInput("123", OrderEvent(1, "CREATED"), timestamp.plusSeconds(60))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "DELETED")

      assert(outputTopic.isEmpty)

      val redirectOutput = redirectTopic.readKeyValue()

      assert(redirectOutput.key == "789")
      assert(redirectOutput.value.key == 1)
      assert(redirectOutput.value.action == "DELETED")
    }

  }

}
