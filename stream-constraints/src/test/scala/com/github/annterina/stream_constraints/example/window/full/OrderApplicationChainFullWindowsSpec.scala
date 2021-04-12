package com.github.annterina.stream_constraints.example.window.full

import java.time.{Duration, Instant}
import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import com.github.annterina.stream_constraints.example.{OrderEvent, OrderEventSerde}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderApplicationChainFullWindowsSpec extends AnyFunSpec with BeforeAndAfterEach {

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

    val updatedCreatedWindow = new WindowConstraintBuilder[String, OrderEvent]
      .before((_, e) => e.action == "UPDATED", "Order Updated")
      .after((_, e) => e.action == "CREATED", "Order Created")
      .window(Duration.ofSeconds(10))
      .swap

    val constraints = new ConstraintBuilder[String, OrderEvent, Integer]
      .windowConstraint(cancelledUpdatedWindow)
      .windowConstraint(updatedCreatedWindow)
      .withFullWindows()
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

  describe("Order Application a chain of full window constraints") {

    it("should detect and swap events in the window") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(2))

      // advances stream time
      inputTopic.pipeInput("789", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(13))

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CANCELLED")
    }

    it("should detect swap events in two windows") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(3))
      inputTopic.pipeInput("789", OrderEvent(1, "CREATED"), timestamp.plusSeconds(6))

      // advances stream time
      inputTopic.pipeInput("000", OrderEvent(1, "NOT_RELATED"), timestamp.plusSeconds(20))

      val output = outputTopic.readKeyValue()

      assert(output.key == "789")
      assert(output.value.key == 1)
      assert(output.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "000")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "NOT_RELATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "456")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "UPDATED")

      val fourthOutput = outputTopic.readKeyValue()

      assert(fourthOutput.key == "123")
      assert(fourthOutput.value.key == 1)
      assert(fourthOutput.value.action == "CANCELLED")
    }

    it("should swap events in two windows with multiple events") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "CANCELLED"), timestamp.plusSeconds(1))
      inputTopic.pipeInput("789", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(3))
      inputTopic.pipeInput("000", OrderEvent(1, "CREATED"), timestamp.plusSeconds(6))
      inputTopic.pipeInput("111", OrderEvent(1, "CREATED"), timestamp.plusSeconds(8))

      // stream time advances
      inputTopic.pipeInput("222", OrderEvent(1, "NOT_RELATED"), timestamp.plusSeconds(20))

      val output = outputTopic.readKeyValue()
      assert(output.key == "000")

      val secondOutput = outputTopic.readKeyValue()
      assert(secondOutput.key == "111")

      val thirdOutput = outputTopic.readKeyValue()
      assert(thirdOutput.key == "222")

      val fourthOutput = outputTopic.readKeyValue()
      assert(fourthOutput.key == "789")

      val fifthOutput = outputTopic.readKeyValue()
      assert(fifthOutput.key == "123")

      val sixthOutput = outputTopic.readKeyValue()
      assert(sixthOutput.key == "456")
    }

    it("should detect one of two possible windows") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"), timestamp.plusSeconds(3))

      // stream time advances
      inputTopic.pipeInput("789", OrderEvent(1, "NOT_RELATED"), timestamp.plusSeconds(16))
      outputTopic.readKeyValue()

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
