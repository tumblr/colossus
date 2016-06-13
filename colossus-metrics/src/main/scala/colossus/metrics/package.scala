package colossus

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


    case class TaggedValue(tags: TagMap, value: Long) {
      def tuple = (tags, value)
    }
    case class SerializedMetric(tagValues: List[TaggedValue])
    type SerializedMetricMap = Map[String, List[TaggedValue]]


    def unserialize(s: SerializedMetricMap): MetricMap = s.map{case (addressString, tagvalues) =>
      MetricAddress(addressString) -> tagvalues.map{_.tuple}.toMap
    }

  }

  implicit class RichMetricMap(val underlying: MetricMap) extends AnyVal {

    def fragments(globalTags: TagMap): Seq[MetricFragment] = underlying.flatMap{case (address, values) =>
      values.map{case (tags, value) => MetricFragment(address, tags ++ globalTags, value)}
    }.toSeq
    def fragments: Seq[MetricFragment] = fragments(TagMap.Empty)

    def withTags(newtags: TagMap): MetricMap = underlying.map{ case (address, values) =>
      address -> values.map{case (tags, value) => (tags ++ newtags, value)}
    }

  }



  implicit class RichTagMap(val underlying: TagMap) extends AnyVal {
    def lineString = underlying.map{case (name, value) => name + ":" + value}.mkString(" ")

    def id = underlying.map{case (k,v) => k + "_" + v}.toSeq.mkString("_")
    def name = underlying.map{case (k,v) => k + ":" + v}.mkString(" ")



  }


}
