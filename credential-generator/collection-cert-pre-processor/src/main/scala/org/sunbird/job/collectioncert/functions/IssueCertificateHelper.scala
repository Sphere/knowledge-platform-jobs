package org.sunbird.job.collectioncert.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Row, TypeTokens}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.collectioncert.domain._
import org.sunbird.job.collectioncert.task.CollectionCertPreProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, ScalaJsonUtil}

import java.text.SimpleDateFormat
import scala.collection.JavaConverters._
import scala.collection.{Map => ScalaMap}

trait IssueCertificateHelper {
    private[this] val logger = LoggerFactory.getLogger(classOf[CollectionCertPreProcessorFn])


    def issueCertificate(event:Event, template: Map[String, String])(cassandraUtil: CassandraUtil, cache:DataCache, contentCache: DataCache, metrics: Metrics, config: CollectionCertPreProcessorConfig, httpUtil: HttpUtil): String = {
        logger.info(s"inside issueCertificate ")
        //validCriteria
        logger.info("issueCertificate i/p event =>"+event)
        val criteria = validateTemplate(template, event.batchId)(config)
        logger.info(s"criteria :: ${criteria} ")
        //validateEnrolmentCriteria
        val certName = template.getOrElse(config.name, "")
        logger.info(s"certName :: ${certName} ")
        val additionalProps: Map[String, List[String]] = ScalaJsonUtil.deserialize[Map[String, List[String]]](template.getOrElse("additionalProps", "{}"))
        val enrolledUser: EnrolledUser = validateEnrolmentCriteria(event, criteria.getOrElse(config.enrollment, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]], certName, additionalProps)(metrics, cassandraUtil, config)
        logger.info(s"enrolledUser :: ${enrolledUser} ")
        //validateAssessmentCriteria
        val assessedUser = validateAssessmentCriteria(event, criteria.getOrElse(config.assessment, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]], enrolledUser.userId, additionalProps)(metrics, cassandraUtil, contentCache, config)
        logger.info(s"assessedUser :: ${assessedUser} ")
        //validateUserCriteria
        val userDetails = validateUser(assessedUser.userId, criteria.getOrElse(config.user, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]], additionalProps)(metrics, config, httpUtil)
        logger.info(s"userDetails :: ${userDetails} ")
        //generateCertificateEvent
        if(userDetails.nonEmpty) {
            generateCertificateEvent(event, template, userDetails, enrolledUser, assessedUser, additionalProps, certName)(metrics, cassandraUtil, config, cache, httpUtil)
        } else {
            logger.info(s"""User :: ${event.userId} did not match the criteria for batch :: ${event.batchId} and course :: ${event.courseId}""")
            null
        }
    }
    
    def validateTemplate(template: Map[String, String], batchId: String)(config: CollectionCertPreProcessorConfig):Map[String, AnyRef] = {
        val criteria = ScalaJsonUtil.deserialize[Map[String, AnyRef]](template.getOrElse(config.criteria, "{}"))
        if(!template.getOrElse("url", "").isEmpty && !criteria.isEmpty && !criteria.keySet.intersect(Set(config.enrollment, config.assessment, config.users)).isEmpty) {
            criteria
        } else {
            throw new Exception(s"Invalid template for batch : ${batchId}")
        }
    }

    def validateEnrolmentCriteria(event: Event, enrollmentCriteria: Map[String, AnyRef], certName: String, additionalProps: Map[String, List[String]])(metrics:Metrics, cassandraUtil: CassandraUtil, config:CollectionCertPreProcessorConfig): EnrolledUser = {
        if(!enrollmentCriteria.isEmpty) {
            val query = QueryBuilder.select().from(config.keyspace, config.userEnrolmentsTable)
              .where(QueryBuilder.eq(config.dbUserId, event.userId)).and(QueryBuilder.eq(config.dbCourseId, event.courseId))
              .and(QueryBuilder.eq(config.dbBatchId, event.batchId))
            val row = cassandraUtil.findOne(query.toString)
            metrics.incCounter(config.dbReadCount)
            val enrolmentAdditionProps = additionalProps.getOrElse(config.enrollment, List[String]())
            if(null != row){
                val active:Boolean = row.getBool(config.active)   
                val issuedCertificates = row.getList(config.issuedCertificates, TypeTokens.mapOf(classOf[String], classOf[String])).asScala.toList
                val isCertIssued = !issuedCertificates.isEmpty && !issuedCertificates.filter(cert => certName.equalsIgnoreCase(cert.getOrDefault(config.name,"").asInstanceOf[String])).isEmpty
                val status = row.getInt(config.status)
                val criteriaStatus = enrollmentCriteria.getOrElse(config.status, 2)
                val oldId = if(isCertIssued && event.reIssue) issuedCertificates.filter(cert => certName.equalsIgnoreCase(cert.getOrDefault(config.name,"").asInstanceOf[String]))
                  .map(cert => cert.getOrDefault(config.identifier, "")).head else ""
                val userId = if(active && (criteriaStatus == status) && (!isCertIssued || event.reIssue)) event.userId else ""
                val issuedOn = row.getTimestamp(config.completedOn)
                val addProps = enrolmentAdditionProps.map(prop => (prop -> row.getObject(prop.toLowerCase))).toMap
                EnrolledUser(userId, oldId, issuedOn, {if(addProps.nonEmpty) Map[String, Any](config.enrollment -> addProps) else Map()})
            } else EnrolledUser("", "")
        } else EnrolledUser(event.userId, "") 
    }

    def validateAssessmentCriteria(event: Event, assessmentCriteria: Map[String, AnyRef], enrolledUser: String, additionalProps: Map[String, List[String]])(metrics:Metrics, cassandraUtil: CassandraUtil, contentCache: DataCache, config:CollectionCertPreProcessorConfig):AssessedUser = {
        if(!assessmentCriteria.isEmpty && !enrolledUser.isEmpty) {
            val filteredUserAssessments = getMaxScore(event)(metrics, cassandraUtil, config, contentCache)
            
            val scoreMap = filteredUserAssessments.map(sc => sc._1 -> (sc._2.head.score * 100 / sc._2.head.totalScore)).toMap
            
            val score:Double = if (scoreMap.nonEmpty) scoreMap.values.max else 0d
            val assessmentAdditionProps = additionalProps.getOrElse(config.assessment, List())
            val addProps = {
                if (assessmentAdditionProps.nonEmpty && assessmentAdditionProps.contains("score")) Map("score" -> scoreMap)
                else Map()
            }
            if(isValidAssessCriteria(assessmentCriteria, score)) {
                AssessedUser(enrolledUser, {if(addProps.nonEmpty) Map[String, Any](config.assessment -> addProps) else Map()})
            } else AssessedUser("")
        } else AssessedUser(enrolledUser)
    }

    def validateUser(userId: String, userCriteria: Map[String, AnyRef], additionalProps: Map[String, List[String]])(metrics:Metrics, config:CollectionCertPreProcessorConfig, httpUtil: HttpUtil) = {
        if(!userId.isEmpty) {
            val url = config.learnerBasePath + config.userReadApi + "/" + userId + "?organisations,roles,locations,declarations,externalIds"
            val result = getAPICall(url, "response")(config, httpUtil, metrics)
            if(userCriteria.isEmpty || userCriteria.size == userCriteria.filter(uc => uc._2 == result.getOrElse(uc._1, null)).size) {
                result
            } else Map[String, AnyRef]()
        } else Map[String, AnyRef]()
    }
    
    def getMaxScore(event: Event)(metrics:Metrics, cassandraUtil: CassandraUtil, config:CollectionCertPreProcessorConfig, contentCache: DataCache):Map[String, Set[AssessmentUserAttempt]] = {
        val contextId = "cb:" + event.batchId
        val query = QueryBuilder.select().column("aggregates").column("agg").from(config.keyspace, config.useActivityAggTable)
          .where(QueryBuilder.eq("activity_type", "Course")).and(QueryBuilder.eq("activity_id", event.courseId))
          .and(QueryBuilder.eq("user_id", event.userId)).and(QueryBuilder.eq("context_id", contextId))

        val rows: java.util.List[Row] = cassandraUtil.find(query.toString)
        metrics.incCounter(config.dbReadCount)
        if(null != rows && !rows.isEmpty) {
            val aggregates: Map[String, Double] = rows.asScala.toList.head
              .getMap("aggregates", classOf[String], classOf[java.lang.Double]).asScala.map(e => e._1 -> e._2.toDouble)
              .toMap
            val agg: Map[String, Double] = rows.asScala.toList.head.getMap("agg", classOf[String], classOf[Integer])
              .asScala.map(e => e._1 -> e._2.toDouble).toMap
            val aggs: Map[String, Double] = agg ++ aggregates
            val userAssessments = aggs.keySet.filter(key => key.startsWith("score:")).map(
                key => {
                    val id= key.replaceAll("score:", "")
                    AssessmentUserAttempt(id, aggs.getOrElse("score:" + id, 0d), aggs.getOrElse("max_score:" + id, 1d))
                }).groupBy(f => f.contentId)
              
            val filteredUserAssessments = userAssessments.filterKeys(key => {
                val metadata = contentCache.getWithRetry(key)
                if (metadata.nonEmpty) {
                    val contentType = metadata.getOrElse("contenttype", "")
                    config.assessmentContentTypes.contains(contentType)
                } else if(!metadata.nonEmpty && config.enableSuppressException){
                        logger.error("Suppressed exception: Metadata cache not available for: " + key)
                        false
                } else throw new Exception("Metadata cache not available for: " + key)
            })
            // TODO: Here we have an assumption that, we will consider max percentage from all the available attempts of different assessment contents.
            if (filteredUserAssessments.nonEmpty) filteredUserAssessments else Map()
        } else Map()
    }

    def isValidAssessCriteria(assessmentCriteria: Map[String, AnyRef], score: Double): Boolean = {
        if(assessmentCriteria.get("score").isInstanceOf[Number]) {
            score == assessmentCriteria.get("score").asInstanceOf[Int].toDouble
        } else {
            val scoreCriteria = assessmentCriteria.getOrElse("score", Map[String, AnyRef]()).asInstanceOf[Map[String, Int]]
            if(scoreCriteria.isEmpty) false
            else {
                val operation = scoreCriteria.head._1
                val criteriaScore = scoreCriteria.head._2.toDouble
                operation match {
                    case "EQ" => score == criteriaScore
                    case "eq" => score == criteriaScore
                    case "=" => score == criteriaScore
                    case ">" => score > criteriaScore
                    case "<" => score < criteriaScore
                    case ">=" => score >= criteriaScore
                    case "<=" => score <= criteriaScore
                    case "ne" => score != criteriaScore
                    case "!=" => score != criteriaScore
                    case _ => false
                }
            }
            
        }
    }
    
    def getAPICall(url: String, responseParam: String)(config:CollectionCertPreProcessorConfig, httpUtil: HttpUtil, metrics: Metrics): Map[String,AnyRef] = {
        val response = httpUtil.get(url, config.defaultHeaders)
        if(200 == response.status) {
            ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
              .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
              .getOrElse(responseParam, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        } else if(400 == response.status && response.body.contains(config.userAccBlockedErrCode)) {
            metrics.incCounter(config.skippedEventCount)
            logger.error(s"Error while fetching user details for ${url}: " + response.status + " :: " + response.body)
            Map[String, AnyRef]()
        } else {
            throw new Exception(s"Error from get API : ${url}, with response: ${response}")
        }
    }

    def getCourseName(courseId: String)(metrics:Metrics, config:CollectionCertPreProcessorConfig, cache:DataCache, httpUtil: HttpUtil): String = {
        val courseMetadata = cache.getWithRetry(courseId)
        if(null == courseMetadata || courseMetadata.isEmpty) {
            val url = config.contentBasePath + config.contentReadApi + "/" + courseId + "?fields=name"
            val response = getAPICall(url, "content")(config, httpUtil, metrics)
            StringContext.processEscapes(response.getOrElse(config.name, "").asInstanceOf[String]).filter(_ >= ' ')
        } else {
            StringContext.processEscapes(courseMetadata.getOrElse(config.name, "").asInstanceOf[String]).filter(_ >= ' ')
        }
    }

    def extract(m: collection.Map[String, AnyRef], key: String): Option[String] = {
        if (m.isDefinedAt(key)) Some(m(key).toString)
        else {
            val res = m.values
              .collect { case ma: Map[String, AnyRef] => extract(ma, key) }
              .dropWhile(_.isEmpty)
            if (res.isEmpty) None else res.head
        }
    }

    def getLastAssessmentScore(courseId: String, userId: String)(metrics: Metrics, cassandraUtil: CassandraUtil, config: CollectionCertPreProcessorConfig, cache: DataCache, httpUtil: HttpUtil): Option[String] = {

        def findLastJsonIdentifier(contentMap: ScalaMap[String, AnyRef]): Option[String] = {

            def collectJsonNodes(node: ScalaMap[String, AnyRef]): Seq[(String, Int)] = {
                val mimeType = node.get("mimeType").map(_.toString)
                val identifier = node.get("identifier").map(_.toString)
                val index = node.get("index") match {
                    case Some(i: java.lang.Integer) => i.intValue()
                    case Some(i: java.lang.Double) => i.toInt
                    case Some(i: java.lang.Long) => i.toInt
                    case Some(i: java.lang.Number) => i.intValue()
                    case _ => 0
                }

                val current = (mimeType, identifier) match {
                    case (Some("application/json"), Some(id)) => Seq((id, index))
                    case _ => Seq.empty
                }

                val children = node.get("children") match {
                    case Some(childList: java.util.List[_]) =>
                        childList.asScala.toSeq.flatMap {
                            case childMap: java.util.Map[_, _] =>
                                collectJsonNodes(childMap.asScala.asInstanceOf[ScalaMap[String, AnyRef]])
                            case childMap: ScalaMap[_, _] =>
                                collectJsonNodes(childMap.asInstanceOf[ScalaMap[String, AnyRef]])
                            case _ => Seq.empty
                        }
                    case Some(childList: Seq[_]) =>
                        childList.flatMap {
                            case childMap: ScalaMap[_, _] =>
                                collectJsonNodes(childMap.asInstanceOf[ScalaMap[String, AnyRef]])
                            case childMap: java.util.Map[_, _] =>
                                collectJsonNodes(childMap.asScala.asInstanceOf[ScalaMap[String, AnyRef]])
                            case _ => Seq.empty
                        }
                    case Some(childList: java.util.Collection[_]) =>
                        childList.asScala.toSeq.flatMap {
                            case childMap: java.util.Map[_, _] =>
                                collectJsonNodes(childMap.asScala.asInstanceOf[ScalaMap[String, AnyRef]])
                            case childMap: ScalaMap[_, _] =>
                                collectJsonNodes(childMap.asInstanceOf[ScalaMap[String, AnyRef]])
                            case _ => Seq.empty
                        }
                    case _ => Seq.empty
                }

                current ++ children
            }

            collectJsonNodes(contentMap)
              .sortBy { case (_, index) => index }
              .lastOption
              .map { case (id, _) => id }
        }

        def formatScoreAsPercentage(value: Float): String = {
            val rounded = BigDecimal(value.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP)
            // Check if the value is a whole number
            if (rounded.remainder(1) == 0) {
                s"${rounded.toInt}%"
            } else {
                s"$rounded%"
            }
        }

        def getMaxScoreFromDB(lastIdentifier: String): Option[String] = {
            val query = QueryBuilder.select().from(config.userAssessmentSummaryKeyspace, config.userAssessmentSummaryTable)
              .where(QueryBuilder.eq("user_id", userId))
              .and(QueryBuilder.eq("content_id", lastIdentifier))
              .allowFiltering()

            val row = cassandraUtil.findOne(query.toString)
            Option(row).map(r => formatScoreAsPercentage(r.getFloat("max_score")))
        }

        val courseHierarchyOpt = Option(cache.getWithRetry("hierarchy_" + courseId))
          .filter(h => h != null && h.nonEmpty)

        val courseHierarchy = courseHierarchyOpt.getOrElse {
            println(s"Cache miss for course: $courseId, fetching from API")
            val url = config.contentBasePath + config.collectionHierarchyReadApi + "/" + courseId
            getAPICall(url, "content")(config, httpUtil, metrics)
        }

        val contentMap: ScalaMap[String, AnyRef] = courseHierarchy match {
            case m: java.util.Map[_, _] => m.asScala.asInstanceOf[ScalaMap[String, AnyRef]]
            case m: ScalaMap[_, _] => m.asInstanceOf[ScalaMap[String, AnyRef]]
            case _ =>
                println(s"Unexpected course hierarchy type: ${courseHierarchy.getClass}")
                return None
        }

        val result = for {
            lastIdentifier <- findLastJsonIdentifier(contentMap)
            _ = println(s"Last JSON identifier found: $lastIdentifier")
            maxScore <- getMaxScoreFromDB(lastIdentifier)
            _ = println(s"Max score retrieved: $maxScore")
        } yield maxScore

        result
    }

    def generateCertificateEvent(event: Event, template: Map[String, String], userDetails: Map[String, AnyRef], enrolledUser: EnrolledUser, assessedUser: AssessedUser, additionalProps: Map[String, List[String]], certName: String)(metrics:Metrics, cassandraUtil: CassandraUtil, config:CollectionCertPreProcessorConfig, cache:DataCache, httpUtil: HttpUtil) = {
        logger.info(s"generateCertificateEvent called ")

    def getCourseOrganisation(courseId: String)(metrics: Metrics, config: CollectionCertPreProcessorConfig, cache: DataCache, httpUtil: HttpUtil): String = {
        val courseMetadata = cache.getWithRetry(courseId)
        var data: String = ""
        if(null == courseMetadata || courseMetadata.isEmpty) {
            val url = config.contentBasePath + config.contentReadApi + "/" + courseId
            val response = getAPICall(url, "content")(config, httpUtil, metrics)
            val orgData = response.get("organisation").toArray
            val pm = orgData(0).toString
            data = pm.substring(1, pm.length-1)
        } else {
            val orgData = courseMetadata.get("organisation").toArray
            val pm = orgData(0).toString
            data = pm.substring(1, pm.length-1)
        }
        data
    }

        val firstName = Option(userDetails.getOrElse("firstName", "").asInstanceOf[String]).getOrElse("")
        val lastName = Option(userDetails.getOrElse("lastName", "").asInstanceOf[String]).getOrElse("")
        def nullStringCheck(name:String):String = {if(StringUtils.equalsIgnoreCase("null", name)) ""  else name}
        val recipientName = nullStringCheck(firstName).concat(" ").concat(nullStringCheck(lastName)).trim
        val courseName = getCourseName(event.courseId)(metrics, config, cache, httpUtil)
        val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
        val profileDetails : Map[String, AnyRef] = userDetails.getOrElse("profileDetails", "").asInstanceOf[Map[String, AnyRef]]
        val profileReq : Map[String, AnyRef] = profileDetails.getOrElse("profileReq", "").asInstanceOf[Map[String, AnyRef]]
        val personalDetails : Map[String, AnyRef] = profileReq.getOrElse("personalDetails", "").asInstanceOf[Map[String, AnyRef]]
        val professionalDetails : List[Map[String, AnyRef]] = profileReq.getOrElse("professionalDetails", Nil).asInstanceOf[List[Map[String, AnyRef]]]
        logger.info(s"personalDetails :: ${personalDetails} ")
        logger.info(s"professionalDetails :: ${professionalDetails} ")
        var orgName: String = "[NA]"
        logger.info(s"orgName :: ${orgName} ")
        if (!professionalDetails.isEmpty) {
            val organizationDetails: Map[String, AnyRef] = professionalDetails.head
            logger.info(s"organizationDetails :: ${organizationDetails} ")
            if (!organizationDetails.isEmpty) {
                orgName = Option(organizationDetails.getOrElse("name", "[NA]").asInstanceOf[String]).getOrElse("[NA]")
                if(orgName.isBlank)
                    orgName = "[NA]"
                logger.info(s"orgName :: ${orgName} ")
            }
        }
        var address = Array[String]()
        var country: String = "[NA]"
        var state: String = "[NA]"
        var district: String = "[NA]"
        val postalAddress = Option(personalDetails.getOrElse("postalAddress", "[NA]").asInstanceOf[String]).getOrElse("[NA]")
        if(!postalAddress.isBlank) {
            logger.info(s"postalAddress :: ${postalAddress} ")
            address = postalAddress.split(", ")
            if (!address.isEmpty && !address.equals("[NA]")) {
                country = address.lift(0).getOrElse("[NA]")
                state = address.lift(1).getOrElse("[NA]")
                district = address.lift(2).getOrElse("[NA]")
                logger.info(s"country :: ${country} ")
                logger.info(s"state :: ${state} ")
                logger.info(s"district :: ${district} ")
            }
        }
        val regNurseRegMidwifeNumber = Option(personalDetails.getOrElse("regNurseRegMidwifeNumber", "[NA]").asInstanceOf[String]).getOrElse("[NA]")
        val maxScore = getLastAssessmentScore(event.courseId, event.userId)(metrics, cassandraUtil, config, cache, httpUtil)

        val related = getRelatedData(event, enrolledUser, assessedUser, userDetails, additionalProps, certName, courseName)(config)
        val providerName = getCourseOrganisation(event.courseId)(metrics, config, cache, httpUtil)
        val eData = Map[String, AnyRef] (
            "issuedDate" -> dateFormatter.format(enrolledUser.issuedOn),
            "data" -> List(Map[String, AnyRef]("recipientName" -> recipientName, "recipientId" -> event.userId)),
            "criteria" -> Map[String, String]("narrative" -> certName),
            "svgTemplate" -> template.getOrElse("url", ""),
            "oldId" -> enrolledUser.oldId,
            "templateId" -> template.getOrElse(config.identifier, ""),
            "userId" -> event.userId,
            "orgId" -> userDetails.getOrElse("rootOrgId", ""),
            "issuer" -> ScalaJsonUtil.deserialize[Map[String, AnyRef]](template.getOrElse(config.issuer, "{}")),
            "signatoryList" -> ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](template.getOrElse(config.signatoryList, "[]")),
            "courseName" -> courseName,
            "basePath" -> config.certBasePath,
            "related" ->  related,
            "name" -> certName,
            "rmNumber" -> regNurseRegMidwifeNumber,
            "orgName" -> orgName,
            "country" -> country,
            "state" -> state,
            "district" -> district,
            "providerName" -> providerName,
            "tag" -> event.batchId,
            "maxScore" -> maxScore
        )

        logger.info(s"edata :: ${eData} ")

        ScalaJsonUtil.serialize(BEJobRequestEvent(edata = eData, `object` = EventObject(id= event.userId)))
    }

    def getLocationDetails(userDetails: Map[String, AnyRef], additionalProps: Map[String, List[String]]): Map[String, Any] = {
        if(additionalProps.getOrElse("location", List()).nonEmpty) {
            val userLocations = userDetails.getOrElse("userLocations", List()).asInstanceOf[List[Map[String, AnyRef]]].map(l => l.getOrElse("type", "").asInstanceOf[String] -> l.getOrElse("name", "").asInstanceOf[String]).toMap
            val locAdditionProps = additionalProps.getOrElse("location", List()).map(prop => prop -> userLocations.getOrElse(prop, null)).filter(p => null != p._2).toMap
            if(locAdditionProps.nonEmpty) Map("location" -> locAdditionProps) else Map()
        }else Map()
    }

    def getRelatedData(event: Event, enrolledUser: EnrolledUser, assessedUser: AssessedUser,
                       userDetails: Map[String, AnyRef], additionalProps: Map[String, List[String]], certName: String, courseName: String)(config: CollectionCertPreProcessorConfig): Map[String, Any] = {
        val userAdditionalProps = additionalProps.getOrElse(config.user, List()).filter(prop => userDetails.contains(prop)).map(prop => (prop -> userDetails.getOrElse(prop, null))).toMap
        val locationProps = getLocationDetails(userDetails, additionalProps) 
        val courseAdditionalProps: Map[String, Any] = if(additionalProps.getOrElse("course", List()).nonEmpty) Map("course" -> Map("name" -> courseName)) else Map()
        Map[String, Any]("batchId" -> event.batchId, "courseId" -> event.courseId, "type" -> certName) ++
          locationProps ++ enrolledUser.additionalProps ++ assessedUser.additionalProps ++ userAdditionalProps ++ courseAdditionalProps
    }
}
