package com.github.annterina.stream_constraints.example

import java.time.{Duration, Instant}
import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderMultipleWindowApplicationSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, OrderEvent] = _
  private var outputTopic: TestOutputTopic[String, OrderEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

    val cancelledUpdatedWindow = new WindowConstraintBuilder[String, OrderEvent]
      .before((_, e) => e.action == "CANCELLED", "Order Cancelled")
      .after((_, e) => e.action == "UPDATED", "Order Updated")
      .window(Duration.ofSeconds(10))
      .swap

    val cancelledCreatedWindow = new WindowConstraintBuilder[String, OrderEvent]
      .before((_, e) => e.action == "UPDATED", "Order Updated")
      .after((_, e) => e.action == "CREATED", "Order Created")
      .window(Duration.ofSeconds(10))
      .swap

    val constraints = new ConstraintBuilder[String, OrderEvent, Integer]
      .windowConstraint(cancelledUpdatedWindow)
      .windowConstraint(cancelledCreatedWindow)
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
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Order Terminal Application With Multiple Window Constraints") {

    it("should detect and swap events in the window") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(2))

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")
    }

    it("should detect multiple before and swap events in the window") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(3))
      inputTopic.pipeInput("789", OrderEvent(1, "CREATED"), timestamp.plusSeconds(6))

      val output = outputTopic.readKeyValue()

      assert(output.key == "789")
      assert(output.value.key == 1)
      assert(output.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "789")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "CANCELLED")
    }
  }
}
