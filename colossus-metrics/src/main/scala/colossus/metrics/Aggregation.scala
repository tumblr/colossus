package colossus.metrics

import scala.util.Try

/**
 * Used to select one or more values on one or more tags
 *
 * eg. Suppose we have a metric "foo" with a tag named "bar" with values "A",
 * "B", "C".  We can pass a selector "foo" -> ["A", "B"] to indicate we wish to
 * select values on the metric where "foo" is "A" or "B"
 */
case class TagSelector(tags: Map[String, Seq[String]]) {

  def matches(map: TagMap): Boolean = tags.find{case (key, values) =>
    ! (map.contains(key) && (values.contains("*") || values.contains(map(key))))
  }.isDefined == false

}

object TagSelector {

  /**
   * expects the string to look like TAG=value1,value2;TAG=*;
   */
  def parseCLI(cmd: String): Try[TagSelector] = Try {
    if (cmd.toUpperCase == "NONE") {
      TagSelector(Map())
    } else {
      val map = cmd.split(";").map{str =>
        val pieces = str.split("=")
        val tag = pieces(0)
        val values = pieces(1).split(",").toSeq
        (tag -> values)
      }.toMap
      TagSelector(map)
    }
  }
}

sealed trait AggregationType {
  def aggregate(values: Seq[MetricValue]): RawMetricValue
}

/**
 * An aggregation type defined how several values for the same metric (each
 * with different tags) get merged into one value.  Notice that these only get
 * called when there is at least one value to aggregate.  The Natural type uses
 * the + operation defined on the MetricValue
 */
object AggregationType {
  case object Sum extends AggregationType {
    def aggregate(values: Seq[MetricValue]): RawMetricValue = values.foldLeft(0L){case (build: RawMetricValue, next: MetricValue) => build + next.value}
  }
  case object Average extends AggregationType {
    def aggregate(values: Seq[MetricValue]) = if (values.size > 0) {
      values.foldLeft(0L){case (build, next) => build + next.value} / values.size
    } else {
      0
    }
  }
  case object Max extends AggregationType {
    def aggregate(values: Seq[MetricValue]) = values.foldLeft(Long.MinValue){case (max, next) => if (max > next.value) max else next.value}
  }
  case object Min extends AggregationType {
    def aggregate(values: Seq[MetricValue]) = values.foldLeft(Long.MaxValue){case (min, next) => if (min < next.value) min else next.value}
  }
  case object Natural extends AggregationType {
    def aggregate(values: Seq[MetricValue]) = values.reduce{_ + _}.value
  }


  def fromString(str: String): Try[AggregationType] = Try {
    str.toUpperCase match {
      case "SUM" => Sum
      case "AVG" => Average
      case "MAX" => Max
      case "MIN" => Min
      case "NAT" | "NATURAL" => Natural
      case _     => throw new IllegalArgumentException(s"Invalid aggregation type $str")
    }
  }
}



case class GroupBy(tags: Set[String], aggregationType: AggregationType) {
  def reduce(map: TagMap): TagMap = map.filter{case (key, value) => tags.contains(key)}

  def group(values: ValueMap): RawValueMap = {
    val builder = collection.mutable.Map[TagMap, collection.mutable.ListBuffer[MetricValue]]()
    values.foreach{case (tags, value) =>
      val reduced = reduce(tags)
      if (! builder.contains(reduced)) {
        builder(reduced) = new collection.mutable.ListBuffer[MetricValue]()
      }
      builder(reduced).prepend(value)
    }
    builder.toMap.map{case (tagmap, valueBuffer) =>
      (tagmap, aggregationType.aggregate(valueBuffer.toSeq))
    }
  }

}

case class MetricValueFilter(filter: Option[TagSelector], aggregate: Option[GroupBy]) {

  def process(values: ValueMap): RawValueMap = {
    val filtered = filter.map{f => values.filter{case (tags, value) => f.matches(tags)}}.getOrElse(values)
    aggregate.map{_.group(filtered)}.getOrElse(filtered.toRawValueMap)
  }

}

object MetricValueFilter {
  val Empty = MetricValueFilter(None, None)
}
