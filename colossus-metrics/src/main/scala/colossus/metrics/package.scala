package colossus

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Future

import net.liftweb.json._

package object metrics {

  //hot dog! look at all these maps!
  type TagMap     = Map[String, String]
  type ValueMap   = Map[TagMap, MetricValue]
  type MetricMap  = Map[MetricAddress, ValueMap]

  type MetricValue = Long
  type MetricSet    = Set[Metric]


  object TagMap {
    val Empty: TagMap = Map()
  }

  object ValueMap {
    val Empty: ValueMap = Map()
  }

  object MetricMap {
    val Empty: MetricMap = Map()
    def apply(metrics: Metric*): MetricMap = Map(metrics.map{m => (m.address -> m.values)}: _*)

    implicit val formats = DefaultFormats

    case class TaggedValue(tags: TagMap, value: Long) {
      def tuple = (tags, value)
    }
    case class SerializedMetric(tagValues: List[TaggedValue])
    type SerializedMetricMap = Map[String, List[TaggedValue]]


    def unserialize(s: SerializedMetricMap): MetricMap = s.map{case (addressString, tagvalues) => 
      MetricAddress(addressString) -> tagvalues.map{_.tuple}.toMap
    }

    def fromJson(j: JValue): MetricMap = unserialize(j.extract[SerializedMetricMap])
  }

  //these value classes are the bees knees!

  implicit class RichMetricMap(val underlying: MetricMap) extends AnyVal {
    /**
     * Inserts the values from the given map into this map, merging any values
     * with the same address, but if two values collide, the value from the
     * given map overwrites the value in this map.  Thus this function is not
     * commutative!
     */
    def <<(given: MetricMap): MetricMap = {
      val builder = collection.mutable.Map[MetricAddress, ValueMap]()
      builder ++= underlying
      given.foreach{ case(address, values) =>
        if (builder contains address) {
          builder(address) = builder(address) ++ values
        } else {
          builder(address) = values
        }
      }
      builder.toMap
    }

    def +(metric: Metric): MetricMap = if (underlying contains metric.address) {
      underlying + (metric.address -> (underlying(metric.address) ++ metric.values))
    } else {
      underlying + (metric.address -> metric.values)
    }

    //this is pretty inefficient, but currently no better way to do it
    def addTags(globalTags: TagMap): MetricMap = underlying.map{case (address, valueMap) => (address, valueMap.addTags(globalTags))}


    def filter(filters: Seq[MetricFilter]): MetricMap = underlying.flatMap{ case (address, values) =>
      filters.find{_.address matches address}.map{filter => (filter.alias.getOrElse(address) -> filter.valueFilter.process(values))}
    }

    def filter(f: MetricFilter): MetricMap = filter(List(f))

    def prefix(address: MetricAddress): MetricMap = underlying.map{case (a, values) => (address / a, values)}

    def fragments(globalTags: TagMap): Seq[MetricFragment] = underlying.flatMap{case (address, values) => 
      values.map{case (tags, value) => MetricFragment(address, tags ++ globalTags, value)}
    }.toSeq
    def fragments: Seq[MetricFragment] = fragments(TagMap.Empty)

    import JsonDSL._

    def toJson: JValue = JObject(
      underlying.map{case (metric, values) => 
        val valueArray = JArray(
          values.map{case (tags, value) => 
            val tagObj = JObject(
              tags.map{ case (key, value) => 
                JField(key, JString(value))
              }.toList
            )
            JObject(List(
              JField("tags", tagObj),
              JField("value", JInt(value))
            ))
          }.toList
        )
        JField(metric.toString, valueArray)
      }.toList
    )

  }



  implicit class RichTagMap(val underlying: TagMap) extends AnyVal {
    def lineString = underlying.map{case (name, value) => name + ":" + value}.mkString(" ")

    def id = underlying.map{case (k,v) => k + "_" + v}.toSeq.mkString("_")
    def name = underlying.map{case (k,v) => k + ":" + v}.mkString(" ")

    

  }

  implicit class RichValueMap(val underlying: ValueMap) extends AnyVal {
    def lineString(indent: Boolean = true): String = underlying.map{ case (tags, value) => 
      (if (indent) "\t" else "") + "[" + tags.lineString + "] " + value.toString
    }.mkString("\n")

    def lineString: String = lineString() //really, scala?

    def filter(filter: MetricValueFilter) = filter.process(underlying)

    def addTags(globalTags: TagMap): ValueMap = underlying.map{case (tags, value) => (tags ++ globalTags, value)}

    def tagNames = underlying.map{case (tags, values) => tags.keys}.reduce{_ ++ _}

  }

  implicit class RichMetricset(val underlying: MetricSet) extends AnyVal {
    def toMap: MetricMap = underlying.map{case Metric(address, values) => (address -> values)}.toMap

  }

  type Timestamp = Long

  object Timestamp {
    def apply(): Timestamp = System.currentTimeMillis / 1000
  }

  /**
   * STAR's are purely for maintaining sanity when actors are being passed all
   * over the place and are intended to help detect at compile time when we're
   * sending a message to the wrong actor.  eg, no more "foo: ActorRef" in parameters
   *
   * It is not intended to ensure an actor can handle the messages that are
   * sent to it.  Perhaps we will switch to typed channels when those are more
   * stable
   */
  trait SemiTypedActorRef[M] {

    val ref: ActorRef

    def !(message: M) {
      ref ! message
    }

    def ?(message: M)(implicit timeout: Timeout): Future[Any] = ref ? message
  }

}
