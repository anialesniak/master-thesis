package com.github.annterina.stream_constraints.example

import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.{Constraint, ConstraintBuilder}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class ExampleApplicationSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, String] = _
  private var outputTopic: TestOutputTopic[String, String] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val constraint: Constraint[String, String, String] = new ConstraintBuilder[String, String, String]
      .atLeastOnce(value => value.split("_")(1) == "created") // TODO .withLink
      .before(value => value.split("_")(1) == "updated") // TODO .withLink
      .valueLink(v => v.split("_").head)(Serdes.String)
      .build(Serdes.String, Serdes.String)

    val builder = new CStreamsBuilder()

    builder
      .stream("topic")(Consumed.`with`(Serdes.String, Serdes.String))
      .filter((k, _) => !k.startsWith("a"))
      .constrain(constraint)
      .to("output-topic")(Produced.`with`(Serdes.String, Serdes.String))

    testDriver = new TopologyTestDriver(builder.build(), config)

    inputTopic = testDriver.createInputTopic(
      "topic",
      Serdes.String.serializer(),
      Serdes.String.serializer()
    )

    outputTopic = testDriver.createOutputTopic(
      "output-topic",
      Serdes.String.deserializer(),
      Serdes.String.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Example Application") {

    it("should emit the prerequisite event") {
      inputTopic.pipeInput("key", "a_created")

      val output = outputTopic.readKeyValue()

      assert(output.key == "key")
      assert(output.value == "a_created")
    }

    it("should buffer an event when the prerequisite was not processed") {
      inputTopic.pipeInput("key", "a_updated")

      assert(outputTopic.isEmpty)
    }

    it("should buffer an event when the prerequisite was not processed and publish not related event") {
      inputTopic.pipeInput("key", "a_updated")
      inputTopic.pipeInput("key", "b_created")

      val output = outputTopic.readKeyValue()

      assert(output.key == "key")
      assert(output.value == "b_created")
    }

    it("should publish both events after receiving the prerequisite") {
      inputTopic.pipeInput("key", "a_updated")
      inputTopic.pipeInput("key", "a_created")

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "key")
      assert(firstOutput.value == "a_created")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "key")
      assert(secondOutput.value == "a_updated")
    }
  }

}
