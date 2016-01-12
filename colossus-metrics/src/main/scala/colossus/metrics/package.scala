package colossus

import net.liftweb.json._

//TODO: break up this whole thing

package object metrics {

  type MetricValue = Long

  //hot dog! look at all these maps!
  type TagMap     = Map[String, String]
  type ValueMap   = Map[TagMap, MetricValue]
  type MetricMap  = Map[MetricAddress, ValueMap]


  object TagMap {
    val Empty: TagMap = Map()
  }

  object ValueMap {
    val Empty: ValueMap = Map()
  }

  object MetricMap {
    val Empty: MetricMap = Map()

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

  implicit class RichMetricMap(val underlying: MetricMap) extends AnyVal {

    def fragments(globalTags: TagMap): Seq[MetricFragment] = underlying.flatMap{case (address, values) =>
      values.map{case (tags, value) => MetricFragment(address, tags ++ globalTags, value)}
    }.toSeq
    def fragments: Seq[MetricFragment] = fragments(TagMap.Empty)


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


}
