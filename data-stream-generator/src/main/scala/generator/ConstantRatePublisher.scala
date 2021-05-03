package generator

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

/**
 * Publishes a constant rate of data on Kafka and sleeps in between the observations from different timestamps.
 * This publisher will also be used as the warm up publisher.
 *
 * @param kafkaProperties
 */

class ConstantRatePublisher(kafkaProperties: Properties) extends Publisher {

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"))
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  override def publish(): Unit = {

    val source = Source.fromResource("data/events.txt")
    for (line <- source.getLines()) {

      val (eventId, policyId, event) = DataUtils.splitLine(line)

      val record = new ProducerRecord[String, String]("policy-events", policyId, event)
      record.headers.add(new RecordHeader("event_id", eventId.getBytes))
      record.headers.add(new RecordHeader("publish_time", System.currentTimeMillis().toString.getBytes))

      producer.send(record, new CompareProducerCallback)
      producer.flush()

      Thread.sleep(5)
    }

    source.close()
  }
}

class CompareProducerCallback extends Callback {
  @Override
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      exception.printStackTrace()
    }
  }
}