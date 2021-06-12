package generator

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

class PolicyPublisher(kafkaProperties: Properties) extends Publisher {

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"))
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  val streamTimeAdvancement = "e1b24bca-b8c8-11eb-8529-0242ac130003 aaaaaaaaaa {\"kind\": \"UpdatePolicyEvent\", \"originator\": \"d433392f-c0a5-431c-b3b7-d54889c3652f\", \"date\": 1621422717, \"customer\": {\"customerId\": \"3c529zzj4u\", \"firstname\": \"Max\", \"lastname\": \"Mustermann\", \"birthday\": 631148400000, \"streetAddress\": \"Oberseestrasse 10\", \"postalCode\": \"8640\", \"city\": \"Rapperswil\", \"email\": \"admin@example.com\", \"phoneNumber\": \"055 222 4111\", \"moveHistory\": [], \"links\": [{\"rel\": \"self\", \"href\": \"http://localhost:8110/customers/rgpp0wkpec?fields=\"}, {\"rel\": \"address.change\", \"href\": \"http://localhost:8110/customers/rgpp0wkpec/address\"}]}, \"policy\": {\"policyId\": \"aaaaaaaaaa\", \"customer\": \"rgpp0wkpec\", \"creationDate\": 1618948272375, \"policyPeriod\": {\"startDate\": 1618956000000, \"endDate\": 1619215200000}, \"policyType\": \"Life Insurance\", \"deductible\": {\"amount\": 100, \"currency\": \"CHF\"}, \"policyLimit\": {\"amount\": 100000, \"currency\": \"CHF\"}, \"insurancePremium\": {\"amount\": 1000, \"currency\": \"CHF\"}, \"insuringAgreement\": {\"agreementItems\": []}, \"links\": [], \"_expandable\": [\"customer\"]}}"

  override def publish(): Unit = {

    val source = Source.fromResource("data/events.txt")
    for (line <- source.getLines()) {

      val (eventId, policyId, event) = DataUtils.splitLine(line)

      val eventWithTime = event.replaceAll("\\b1621422717\\b", System.currentTimeMillis().toString)
      val eventWithTimeAndId = eventWithTime.replaceAll("\\bPolicyManagementBackend\\b", eventId)

      val record = new ProducerRecord[String, String]("policy-events", policyId, eventWithTimeAndId)

      producer.send(record, new CompareProducerCallback)
      producer.flush()

      Thread.sleep(1)
    }

    Thread.sleep(1000)

    val (_, policyId, event) = DataUtils.splitLine(streamTimeAdvancement)
    val eventWithTime = event.replaceAll("\\b1621422717\\b", System.currentTimeMillis().toString)
    val record = new ProducerRecord[String, String]("policy-events", policyId, eventWithTime)

    producer.send(record, new CompareProducerCallback)
    producer.flush()

    source.close()
  }
}