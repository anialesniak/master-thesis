package com.github.annterina.stream_constraints.example.window

import java.time.{Duration, Instant}
import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import com.github.annterina.stream_constraints.example.{OrderEvent, OrderEventSerde}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderApplicationWindowSpec extends AnyFunSpec with BeforeAndAfterEach {

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

    val constraints = new ConstraintBuilder[String, OrderEvent, Integer]
      .windowConstraint(cancelledUpdatedWindow)
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

  describe("Order Application with a window constraint") {

    it("should detect and swap events in the window despite the decreased timestamp") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp.plusSeconds(2))
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp)

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")
    }

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
      inputTopic.pipeInput("456", OrderEvent(1, "CANCELLED"), timestamp.plusSeconds(1))
      inputTopic.pipeInput("789", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(2))

      val output = outputTopic.readKeyValue()

      assert(output.key == "789")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "456")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "CANCELLED")
    }

    it("should emit the before event when the stream time advances") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "CREATED"), timestamp.plusSeconds(10))
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(14))

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")
    }

    it("should publish before event that dropped out of window after swap") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "CANCELLED"), timestamp.plusSeconds(5))
      inputTopic.pipeInput("789", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(12))

      val output = outputTopic.readKeyValue()

      assert(output.key == "789")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "123")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "CANCELLED")
    }

    it("should publish before event that dropped out of window before swap") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "CANCELLED"), timestamp.plusSeconds(10))
      inputTopic.pipeInput("789", OrderEvent(1, "CANCELLED"), timestamp.plusSeconds(11))
      inputTopic.pipeInput("000", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(12))

      val output = outputTopic.readKeyValue()

      assert(output.key == "123")
      assert(output.value.key == 1)
      assert(output.value.action == "CANCELLED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "000")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "456")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "CANCELLED")

      val fourthOutput = outputTopic.readKeyValue()

      assert(fourthOutput.key == "789")
      assert(fourthOutput.value.key == 1)
      assert(fourthOutput.value.action == "CANCELLED")
    }

    it("should ignore next after events after the first one") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(2))
      inputTopic.pipeInput("789", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(5))

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "789")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "UPDATED")
    }

    it("should ignore action when window is not detected") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(11))

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")
    }
  }
}
