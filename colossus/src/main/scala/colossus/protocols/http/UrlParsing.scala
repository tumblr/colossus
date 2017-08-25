package colossus.protocols.http

/**
  * URL parsing can be done using one of two paths. The first uses an object based decomposition with a minimal number
  * of string operations. This parses using the 'on' keyword and the left associative '/' operator.
  *
  * The second method uses string operations in a right associative manner. Because all values are strings, it is less type
  * safe than the object method. This is used with the 'in' keyword and the right associative '/:' operator. In this method,
  * the final variable in the pattern will consume all remaining levels.
  *
  * To use either language, import the UrlParsing._ object
  */
object UrlParsing {

  /** Trait encapsulating parts of the parse of a url */
  sealed trait UrlComponent

  /** Case class encapsulating a single piece of a parsed url */
  case class ParsedUrl(pieces: List[String]) extends UrlComponent

  /** Case object indicting the root of a url */
  case object Root extends UrlComponent

  object Url {

    /** Parses a string url into pieces for the object based url parsing DSL */
    def parse(str: String): UrlComponent = {
      val paramless = str.split("\\?").headOption
      paramless
        .map { x =>
          val p = x.split("/").filter { _ != "" }.reverse.toList
          if (p.size == 0) {
            Root
          } else {
            ParsedUrl(p)
          }
        }
        .getOrElse(Root)
    }
  }

  /** Extractor for left-binding object based DSL. To use:
    * case url @ Get <or other methed> on Root / first level / second level / .. repeat
    */
  object / {
    def unapply(p: UrlComponent): Option[(UrlComponent, String)] = p match {
      case ParsedUrl(items) if (items.size == 1) => Some((Root, items.head))
      case ParsedUrl(items) if (items.size > 1)  => Some((ParsedUrl(items.tail), items.head))
      case _                                     => None
    }
  }

  /** Extractor for integers */
  object Integer {
    def unapply(s: String): Option[Int] =
      try {
        Some(s.toInt)
      } catch {
        case n: java.lang.NumberFormatException => None
      }
  }

  /** Extractor for Longs */
  object Long {
    def unapply(s: String): Option[Long] =
      try {
        Some(s.toLong)
      } catch {
        case n: java.lang.NumberFormatException => None
      }
  }

  /**
    * Keyword which triggers left-binding object based parsing. Must be used to match a url pattern with an HTTP method.
    */
  case object on {
    def unapply(request: HttpRequest): Option[(HttpMethod, UrlComponent)] =
      Some(request.head.method, Url.parse(request.head.url))
  }
  object Strings {

    /** Constant indicating the root of the URL for use in string based parsing */
    val Root = "/"

    /** Extractor for right-binding string based DSL. To use:
      * case irl @ Get <or other method> in ROOT /: first level /: second level /: remainder
      */
    object /: {
      def unapply(s: String): Option[(String, String)] = s match {
        case url: String if (url == "/")         => Some((url, ""))
        case url: String if (url startsWith "/") => Some("/", url drop 1)
        case url: String =>
          val last = url.indexOf('/')
          val tail = url.substring(last + 1)
          val head = url.substring(0, last)
          Some((if (head.isEmpty) "/" else head, tail))
      }
    }

    /**
      * Keyword which triggers right-binding string based parsing. Must be used to match a url pattern with an HTTP method.
      */
    case object on {
      def unapply(request: HttpRequest): Option[(HttpMethod, String)] = {
        val component = request.head.url match {
          case ""          => "/"
          case url: String => url
        }
        Some(request.head.method, component)
      }
    }
  }
}
