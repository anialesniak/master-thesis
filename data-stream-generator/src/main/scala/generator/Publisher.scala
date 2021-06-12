package generator

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.slf4j.{Logger, LoggerFactory}

trait Publisher {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def publish(): Unit
}

class CompareProducerCallback extends Callback {
  @Override
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      exception.printStackTrace()
    }
  }
}