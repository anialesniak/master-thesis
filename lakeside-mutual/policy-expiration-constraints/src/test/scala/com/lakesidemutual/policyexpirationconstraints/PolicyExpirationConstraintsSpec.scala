package com.lakesidemutual.policyexpirationconstraints

import java.time.{Duration, Instant}
import java.util.{Date, Properties}

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class PolicyExpirationConstraintsSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var expirationInputTopic: TestInputTopic[String, InsuranceQuoteEvent] = _
  private var policyCreationInputTopic: TestInputTopic[String, InsuranceQuoteEvent] = _
  private var outputTopic: TestOutputTopic[String, InsuranceQuoteEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "policy-expiration-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val insuranceQuoteEventSerde = Serdes.serdeFrom(InsuranceQuoteEventSerde.serializer(),
      InsuranceQuoteEventSerde.deserializer())

    val windowConstraint = new WindowConstraintBuilder[String, InsuranceQuoteEvent]
      .before(((_, e) => e.isInstanceOf[InsuranceQuoteExpiredEvent], "insurance-expired"))
      .after(((_, e) => e.isInstanceOf[PolicyCreatedEvent], "policy-created"))
      .window(Duration.ofSeconds(20))
      .dropBefore

    val constraint = new ConstraintBuilder[String, InsuranceQuoteEvent, java.lang.Long]
      .windowConstraint(windowConstraint)
      .link((_, e) => e.getInsuranceQuoteRequestId)(Serdes.Long)
      .build(Serdes.String, insuranceQuoteEventSerde)

    val builder = new CStreamsBuilder()

    builder
      .stream(Set("insurance-quote-expired-events", "policy-created-events"))(Consumed.`with`(Serdes.String, insuranceQuoteEventSerde))
      .constrain(constraint)
      .to("policy-creation-expiration-events")(Produced.`with`(Serdes.String, insuranceQuoteEventSerde))

    testDriver = new TopologyTestDriver(builder.build(), config)

    expirationInputTopic = testDriver.createInputTopic(
      "insurance-quote-expired-events",
      Serdes.String.serializer(),
      InsuranceQuoteEventSerde.serializer()
    )

    policyCreationInputTopic = testDriver.createInputTopic(
      "policy-created-events",
      Serdes.String.serializer(),
      InsuranceQuoteEventSerde.serializer()
    )

    outputTopic = testDriver.createOutputTopic(
      "policy-creation-expiration-events",
      Serdes.String.deserializer(),
      InsuranceQuoteEventSerde.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Policy Expiration Constraints application") {

    it("should drop the expiration event in the window") {
      val insuranceQuoteId = 123456L
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")

      val insuranceQuoteExpiredEvent = new InsuranceQuoteExpiredEvent(new Date(), insuranceQuoteId)
      val policyCreatedEvent = new PolicyCreatedEvent(new Date(), insuranceQuoteId, "policyId")

      expirationInputTopic.pipeInput(insuranceQuoteId.toString, insuranceQuoteExpiredEvent, timestamp)
      policyCreationInputTopic.pipeInput(insuranceQuoteId.toString, policyCreatedEvent, timestamp.plusSeconds(5))

      val output = outputTopic.readKeyValue()

      assert(output.key == insuranceQuoteId.toString)
      assert(output.value.isInstanceOf[PolicyCreatedEvent])

      assert(outputTopic.isEmpty)
    }
  }

}
