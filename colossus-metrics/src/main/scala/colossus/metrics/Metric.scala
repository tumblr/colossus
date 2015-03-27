package colossus.metrics

import scala.util.Try

trait MetricFormatter[T] {
  def format(m: MetricFragment, timestamp: Long): T
}

object OpenTsdbFormatter extends MetricFormatter[String] { 

  def formatTags(t: TagMap) = t.map{case (k,v) => k + "=" + v}.mkString(" ")

  def format(m: MetricFragment, timestamp: Long): String = s"put ${m.address.pieceString} $timestamp ${m.value} ${formatTags(m.tags)}\n" 
}


case class MetricAddress(components: List[String]) {
  def /(c: String): MetricAddress = copy(components = components :+ c)//.toLowercase removed for backwards compatability

  def /(m: MetricAddress): MetricAddress = copy(components = components ++ m.components)

  override def toString = "/" + components.mkString("/")

  def pieceString = components.mkString("/")

  def idString = components.mkString("_")

  def tail = copy(components.tail)
  def head = components.head
  def size = components.size

  /**
   * selector    -     address
   * /foo/bar matches /foo/bar but not /foo/bar/baz
   * /foo/ * matches foo/bar and foo/bar/baz
   * /foo/ * /baz matches foo/bar/baz but not foo/bar
   */
  def matches(address: MetricAddress): Boolean = {
    def next(mine: List[String], theirs: List[String], lastWasWC: Boolean = false): Boolean = (mine.headOption, theirs.headOption) match {
      case (Some(m), Some(t)) => if (m == t || m == "*") {
        next(mine.tail, theirs.tail, m == "*")
      } else {
        false
      }
      case (None, Some(t)) => if (lastWasWC) true else false
      case (None, None) => true
      case (Some(m), None) => false
    }
    next(components, address.components)
  }

  def startsWith(address: MetricAddress) = components.take(address.components.size) == address.components

  //note, this doesn't actually check if this starts with the address
  def after(address: MetricAddress) = copy(components.drop(address.components.size))
}

object MetricAddress {
  val Root = MetricAddress(Nil)

  def fromString(str: String): Try[MetricAddress] = Try {
    if (str == "/") MetricAddress.Root else {
      val pieces = str.split("/")
      if (pieces(0) != "") {
        throw new IllegalArgumentException("Address must start with leading /")
      }
      MetricAddress(pieces.toList.tail)
    }
  }

  def apply(str: String) = fromString(str).get

  implicit def string2Address(s: String) : MetricAddress = fromString(if (s startsWith "/") s else "/" + s).get

}

case class MetricFragment(address: MetricAddress, tags: TagMap, value: RawMetricValue)


/**
 * Anything that eventually expands to a set of raw stats extends this
 */
trait MetricProducer {
  def metrics(context: CollectionContext): MetricMap
}

/**
 * Any metric that needs to be ticked by the external clock
 */
trait TickedMetric {
  def tick()
}

