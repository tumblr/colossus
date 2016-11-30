package colossus.metrics

trait MetricFormatter[T] {
  def format(m: MetricFragment, timestamp: Long): T
}

object OpenTsdbFormatter extends MetricFormatter[String] {

  def formatTags(t: TagMap) = t.map{case (k,v) => k + "=" + v}.mkString(" ")

  def format(m: MetricFragment, timestamp: Long): String = s"put ${m.address.pieceString} $timestamp ${m.value} ${formatTags(m.tags)}\n"
}


case class MetricAddress(components: List[String]) {
  def /(c: String): MetricAddress = /(MetricAddress(c))

  def /(m: MetricAddress): MetricAddress = copy(components = components ++ m.components)

  override def toString = "/" + components.mkString("/")

  def pieceString = components.mkString("/")

  def idString = components.mkString("_")

  /**
   * String version the address used for loading configuration
   */
  def configString = components.mkString(".")

  def tail = copy(components.tail)
  def head = components.head
  def size = components.size

  /**
   * selector    -     address
   * /foo/bar matches /foo/bar but not /foo/bar/baz
   * /foo/ * matches foo/bar and foo/bar/baz
   * /foo/ * /baz matches foo/bar/baz but not foo/bar
   *
   * PS: This is not commutative
   * @param address MetricAddress the address being checked
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

  def fromString(str: String): MetricAddress = {
    str match {
      case "" | "/" => MetricAddress.Root
      case _ => {
        val s = if (str startsWith "/") str else "/" + str
        val pieces = s.split("/")
        MetricAddress(pieces.toList.tail)
      }
    }
  }

  def apply(str: String) = fromString(str)

  implicit def string2Address(s: String) : MetricAddress = fromString(s)

}

case class MetricFragment(address: MetricAddress, tags: TagMap, value: MetricValue)


