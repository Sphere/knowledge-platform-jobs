package org.sunbird.job.transaction.functions

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.spec.BaseTestSpec

import java.util

/**
 * Legacy object types listed in restrict.objectTypes (test.conf lists Misconception,
 * EventSet...) have no schema on this platform. The audit generator fails on them, so
 * the router must not send them on, while every other event still goes through.
 */
class TransactionEventRouterRestrictSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] =
    TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])

  private val config: Config = ConfigFactory.load("test.conf")
  private val jobConfig = new TransactionEventProcessorConfig(config)
  private val router = new TransactionEventRouter(jobConfig)

  private def event(objectType: String): Event = {
    val map = new util.HashMap[String, Any]()
    map.put("nodeUniqueId", "32920")
    map.put("operationType", "UPDATE")
    map.put("graphId", "domain")
    if (objectType != null) map.put("objectType", objectType)
    new Event(map, 0, 28365L)
  }

  "an event whose objectType is restricted" should "not be routed to audit" in {
    router.isRestricted(event("Misconception")) should be(true)
    router.isRestricted(event("EventSet")) should be(true)
  }

  "an event for content" should "still be routed" in {
    router.isRestricted(event("Content")) should be(false)
    router.isRestricted(event("Collection")) should be(false)
    router.isRestricted(event("Question")) should be(false)
  }

  "an event without an objectType" should "not be treated as restricted" in {
    router.isRestricted(event(null)) should be(false)
  }

  "the restrict check" should "match the configured spelling exactly" in {
    router.isRestricted(event("misconception")) should be(false)
  }
}
