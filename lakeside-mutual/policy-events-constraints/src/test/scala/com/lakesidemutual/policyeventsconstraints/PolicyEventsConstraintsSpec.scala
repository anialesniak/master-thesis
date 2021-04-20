package com.lakesidemutual.policyeventsconstraints

import java.time.{Duration, Instant}
import java.util.{Date, Properties}

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import com.lakesidemutual.policyeventsconstraints.dto.PolicyDto
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class PolicyEventsConstraintsSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, PolicyDomainEvent] = _
  private var outputTopic: TestOutputTopic[String, PolicyDomainEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val policyEventSerde = Serdes.serdeFrom(PolicyEventSerde.serializer(), PolicyEventSerde.deserializer())

    val windowConstraint = new WindowConstraintBuilder[String, PolicyDomainEvent]
      .before(((_, e) => e.`type` == "DeletePolicyEvent", "policy-deleted"))
      .after(((_, e) => e.`type` == "UpdatePolicyEvent", "policy-updated"))
      .window(Duration.ofSeconds(20))
      .swap

    val constraint = new ConstraintBuilder[String, PolicyDomainEvent, String]
      .prerequisite(((_, e) => e.`type` == "UpdatePolicyEvent", "policy-updated"),
        ((_, e) => e.`type` == "DeletePolicyEvent", "policy-deleted"))
      .windowConstraint(windowConstraint)
      .terminal(((_, e) => e.`type` == "DeletePolicyEvent", "policy-deleted"))
      .link((_, e) => e.policyId)(Serdes.String)
      .build(Serdes.String, policyEventSerde)

    val builder = new CStreamsBuilder()

    builder
      .stream("policy-events")(Consumed.`with`(Serdes.String, policyEventSerde))
      .constrain(constraint)
      .to("policy-events-constrained")(Produced.`with`(Serdes.String, policyEventSerde))

    testDriver = new TopologyTestDriver(builder.build(), config)

    inputTopic = testDriver.createInputTopic(
      "policy-events",
      Serdes.String.serializer(),
      PolicyEventSerde.serializer()
    )

    outputTopic = testDriver.createOutputTopic(
      "policy-events-constrained",
      Serdes.String.deserializer(),
      PolicyEventSerde.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Policy Events Constraints application") {

    it("should swap the events in the window") {
      val policyId = "xmhswhgleo"
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")

      val policyDeleteEvent = new DeletePolicyEvent("origin", Date.from(Instant.now), policyId)

      val policyUpdateString = "{\"kind\":\"UpdatePolicyEvent\",\"originator\":\"127.0.0.1\",\"date\":1618948284615,\"customer\":{\"customerId\":\"rgpp0wkpec\",\"firstname\":\"Max\",\"lastname\":\"Mustermann\",\"birthday\":631148400000,\"streetAddress\":\"Oberseestrasse 10\",\"postalCode\":\"8640\",\"city\":\"Rapperswil\",\"email\":\"admin@example.com\",\"phoneNumber\":\"055 222 4111\",\"moveHistory\":[],\"links\":[{\"rel\":\"self\",\"href\":\"http://localhost:8110/customers/rgpp0wkpec?fields=\"},{\"rel\":\"address.change\",\"href\":\"http://localhost:8110/customers/rgpp0wkpec/address\"}]},\"policy\":{\"policyId\":\"xmhswhgleo\",\"customer\":\"rgpp0wkpec\",\"creationDate\":1618948272375,\"policyPeriod\":{\"startDate\":1618956000000,\"endDate\":1619215200000},\"policyType\":\"Life Insurance\",\"deductible\":{\"amount\":1,\"currency\":\"CHF\"},\"policyLimit\":{\"amount\":1111111,\"currency\":\"CHF\"},\"insurancePremium\":{\"amount\":11,\"currency\":\"CHF\"},\"insuringAgreement\":{\"agreementItems\":[]},\"links\":[],\"_expandable\":[\"customer\"]}}"
      val policyUpdateEvent = PolicyEventSerde.deserializer().deserialize("", policyUpdateString.getBytes())

      inputTopic.pipeInput(policyId, policyDeleteEvent, timestamp)
      inputTopic.pipeInput(policyId, policyUpdateEvent, timestamp.plusSeconds(5))

      val output = outputTopic.readKeyValue()

      assert(output.key == policyId)
      assert(output.value.`type`() == "UpdatePolicyEvent")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == policyId)
      assert(secondOutput.value.`type`() == "DeletePolicyEvent")
    }
  }

}
