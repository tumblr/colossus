package colossus
package protocols.http

object UrlParsing {

  sealed trait UrlComponent
  case class ParsedUrl(pieces: List[String]) extends UrlComponent
  case object Root extends UrlComponent

  object Url {
    def parse(str: String): UrlComponent = {
      val paramless = str.split("\\?").headOption
      paramless.map{ x =>
        val p = x.split("/").filter{_ != ""}.reverse.toList
        if (p.size == 0) {
          Root
        } else {
          ParsedUrl(p)
        }
      }.getOrElse(Root)
    }
  }

  object / {
    def unapply(p: UrlComponent): Option[(UrlComponent, String)] = p match {
      case ParsedUrl(items) if (items.size == 1) => Some((Root, items.head))
      case ParsedUrl(items) if (items.size > 1) => Some((ParsedUrl(items.tail), items.head))
      case _ => None
    }
  }

  object Integer {
    def unapply(s: String) : Option[Int] = try {
      Some(s.toInt)
    } catch {
      case n: java.lang.NumberFormatException => None
    }
  }
  
  case object on {
    def unapply(request: HttpRequest): Option[(HttpMethod, UrlComponent)] = Some(request.head.method, Url.parse(request.head.url))
  }

}
