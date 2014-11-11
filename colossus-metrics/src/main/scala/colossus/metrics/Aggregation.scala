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
  def aggregate(values: Seq[Long]): Long
}
object AggregationType {
  case object Sum extends AggregationType {
    def aggregate(values: Seq[Long]) = values.sum
  }
  case object Average extends AggregationType {
    def aggregate(values: Seq[Long]) = values.sum / values.size
  }
  case object Max extends AggregationType {
    def aggregate(values: Seq[Long]) = values.max
  }
  case object Min extends AggregationType {
    def aggregate(values: Seq[Long]) = values.min
  }


  def fromString(str: String): Try[AggregationType] = Try {
    str.toUpperCase match {
      case "SUM" => Sum
      case "AVG" => Average
      case "MAX" => Max
      case "MIN" => Min
      case _     => throw new IllegalArgumentException(s"Invalid aggregation type $str")
    }
  }
}



case class GroupBy(tags: Set[String], aggregationType: AggregationType) {
  def reduce(map: TagMap): TagMap = map.filter{case (key, value) => tags.contains(key)}

  def group(values: ValueMap): ValueMap = {
    val builder = collection.mutable.Map[TagMap, collection.mutable.ListBuffer[Long]]()
    values.foreach{case (tags, value) => 
      val reduced = reduce(tags) 
      if (! builder.contains(reduced)) {
        builder(reduced) = new collection.mutable.ListBuffer[Long]()
      }
      builder(reduced).prepend(value)
    }
    builder.toMap.map{case (tagmap, valueBuffer) =>
      (tagmap, aggregationType.aggregate(valueBuffer.toSeq))
    }
  }     
    
}

case class MetricValueFilter(filter: Option[TagSelector], aggregate: Option[GroupBy]) {

  def process(values: ValueMap): ValueMap = {
    val filtered = filter.map{f => values.filter{case (tags, value) => f.matches(tags)}}.getOrElse(values)
    aggregate.map{_.group(filtered)}.getOrElse(filtered)
  }
  
}

object MetricValueFilter {
  val Empty = MetricValueFilter(None, None)
}

/**
 * A metric filter is essentially a selector for a metric along with a filter
 * for processing/aggregating the values
 */
case class MetricFilter(address: MetricAddress, valueFilter: MetricValueFilter, alias: Option[MetricAddress] = None)

object MetricFilter {

  def apply(address: MetricAddress, filterTags: TagSelector): MetricFilter = 
    MetricFilter(address, MetricValueFilter(Some(filterTags), None))
  def apply(address: MetricAddress, groupTags: Set[String], aggregationType: AggregationType): MetricFilter = 
    MetricFilter(address, MetricValueFilter(None, Some(GroupBy(groupTags, aggregationType))))
  def apply(address: MetricAddress): MetricFilter = 
    MetricFilter(address, MetricValueFilter.Empty)

  def apply(string: String) = MetricFilterParser.parseFilter(string).getOrElse(throw new Exception(s"Illegal Metric filter query: '$string'"))

}

object MetricFilters { 
 def apply(whitelist: MetricAddress*): Seq[MetricFilter] = whitelist.map{a => MetricFilter(a)}
}

import scala.util.parsing.combinator._

object MetricFilterParser extends RegexParsers {

  def parseFilter(in: String): Try[MetricFilter] = parse(query, in) match {
    case Success(result, _) => scala.util.Success(result)
    case n: NoSuccess => scala.util.Failure(new java.lang.Error(n.msg))
  }

  def query = keyword("SELECT") ~> address ~ opt(tagFilter) ~ opt(groupBy) ^^ {
    case (address, aliasOpt) ~ fOpt ~ gOpt => MetricFilter(address, MetricValueFilter(fOpt, gOpt), aliasOpt)
  }

  def address: Parser[(MetricAddress, Option[MetricAddress])] = aliasedAddress | multiAddress

  def aliasedAddress = singleAddress ~ opt(alias) ^^{ case address ~ aliasOpt => (address, aliasOpt)}

  def singleAddress = "/" ~> rep1sep(singleAddressComponent, "/") ^^ {case cmps => MetricAddress(cmps)}
  def singleAddressComponent = name

  def alias = keyword("AS") ~> singleAddress

  def multiAddress: Parser[(MetricAddress, Option[MetricAddress])] = "/" ~> rep1sep(multiAddressComponent, "/") ^^ {case cmps => (MetricAddress(cmps), None)}
  def multiAddressComponent = "*" | name




  def tagFilter = keyword("WHERE") ~> rep1sep(singleTagFilter, ";") ^^ {case kvs => TagSelector(kvs.toMap)}
  def singleTagFilter = name ~ "=" ~ rep1sep(name, ",") ^^ {case key ~ eq ~ values => key -> values.toSeq
  }

  def groupBy = keyword("GROUP") ~> keyword("BY") ~> tagList ~ name ^^ {case keys ~ aggType => GroupBy(keys.toSet, AggregationType.fromString(aggType).get)}
  def tagList = wildcardTag | rep1sep(name, ",")
  def wildcardTag = "*" ^^^ {List("*")} //this works because grouping by any tag that no value has will just collapse everything


  def name = "[a-zA-Z0-9-_.]+".r
  def keyword(word: String) = s"(?iu)${word}".r

}

