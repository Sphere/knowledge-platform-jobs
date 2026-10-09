package org.sunbird.job.transaction.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.service.TransactionEventProcessorService
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.{BaseProcessFunction, Metrics}

class TransactionEventRouter(config: TransactionEventProcessorConfig)(implicit
    mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
    stringTypeInfo: TypeInformation[String]
) extends BaseProcessFunction[Event, String](config)
    with TransactionEventProcessorService {

  private[this] lazy val logger =
    LoggerFactory.getLogger(classOf[TransactionEventRouter])

  override def metricsList(): List[String] = {
    List(
      config.totalEventsCount,
      config.successEventCount,
      config.failedEventCount,
      config.skippedEventCount,
      config.emptySchemaEventCount,
      config.emptyPropsEventCount
    )
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(
      event: Event,
      context: ProcessFunction[Event, String]#Context,
      metrics: Metrics
  ): Unit = {
    try {
      metrics.incCounter(config.totalEventsCount)
      if (isRestricted(event)) {
        logger.info(s"Event not qualified for audit for Identifier : ${event.nodeUniqueId} (objectType ${event.objectType} is restricted).")
        metrics.incCounter(config.skippedEventCount)
      } else if (event.transactionEventProcessorIsValid) {
        logger.info("Valid event -> " + event.nodeUniqueId)
        context.output(config.outputTag, event)
      } else metrics.incCounter(config.skippedEventCount)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(
          ex.getMessage,
          Map("partition" -> event.partition, "offset" -> event.offset),
          ex
        )
    }
  }

  /**
   * Object types listed in restrict.objectTypes are already kept out of search by
   * SearchIndexerRouter. They are kept out of the audit, obsrv and audit-history
   * branches here too: these are legacy types (Domain, Concept, Misconception...)
   * with no schema on this platform, and the audit generator fails with a
   * NullPointerException on an object type it has no definition for -- which stops
   * the whole job on the first such event.
   */
  private[functions] def isRestricted(event: Event): Boolean = {
    val objectType = event.objectType
    objectType != null && config.restrictObjectTypes.contains(objectType)
  }
}
