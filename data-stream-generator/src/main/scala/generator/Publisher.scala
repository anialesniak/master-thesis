package generator

import org.slf4j.{Logger, LoggerFactory}

trait Publisher {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def publish(): Unit
}
