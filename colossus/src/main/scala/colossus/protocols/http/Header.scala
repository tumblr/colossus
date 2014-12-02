package colossus
package protocols.http

import core._
import service._

import akka.util.ByteString
import com.github.nscala_time.time.Imports._

import scala.collection.immutable.HashMap


sealed abstract class HttpMethod(val name: String)
object HttpMethod {
  case object Get     extends HttpMethod("GET")
  case object Post    extends HttpMethod("POST")
  case object Put     extends HttpMethod("PUT")
  case object Delete  extends HttpMethod("DELETE")
  case object Head    extends HttpMethod("HEAD")
  case object Options extends HttpMethod("OPTIONS")
  case object Trace   extends HttpMethod("TRACE")
  case object Connect extends HttpMethod("CONNECT")
  case object Patch   extends HttpMethod("PATCH")

  val methods: List[HttpMethod] = List(Get, Post, Put, Delete, Head, Options, Trace, Connect, Patch)

  def apply(str: String): HttpMethod = methods.find{_.name == str.toUpperCase}.getOrElse(
    throw new HttpParsingException(s"Invalid http method $str")
  )
}

sealed abstract class HttpVersion(versionString: String) {
  override def toString = versionString
  val messageBytes = ByteString("HTTP/" + versionString)
}
object HttpVersion {
  case object `1.0` extends HttpVersion("1.0")
  case object `1.1` extends HttpVersion("1.1")

  def apply(str: String): HttpVersion = {
    if (str == "HTTP/1.1") `1.1` else if (str=="HTTP/1.0") `1.0` else throw new parsing.ParseException(s"Invalid http version '$str'")
  }
}

object HttpHeaders {
  /**
   * by default we're going to disallow multivalues on everything except those
   * defined below
   */
  val allowedMultiValues = List(
    "set-cookie"
  )

  //make these all lower-case

  val Accept            = "accept"
  val ContentLength     = "content-length"
  val Connection        = "connection"
  val SetCookie         = "set-cookie"
  val CookieHeader      = "cookie"
}

case class Cookie(name: String, value: String, expiration: Option[DateTime])

object Cookie {
  def parseHeader(line: String): List[Cookie] = {
    val keyvals: Map[String, String] = line.split(";").map{c => c.trim.split("=", 2).map{i => i.trim}.toList match {
      case key :: value :: Nil => (key.toLowerCase -> value)
      case key :: Nil => key.toLowerCase -> "true"
      case _ => throw new InvalidRequestException(s"Invalid data in cookies")
    }}.toMap

    val cookieVals = keyvals.filter{case (k,v) => k != "expires"}
    val expiration = keyvals.collectFirst{case (k,v) if (k == "expires") => parseCookieExpiration(v)}
    cookieVals.map{case (key, value) => Cookie(key, value, expiration)}.toList
  }

  val CookieExpirationFormat = org.joda.time.format.DateTimeFormat.forPattern("E, dd MMM yyyy HH:mm:ss z")
  def parseCookieExpiration(str: String): DateTime = {
    org.joda.time.DateTime.parse(str, CookieExpirationFormat )
  }
}

case class HttpHead(method: HttpMethod, url: String, version: HttpVersion, headers: List[(String, String)]) {
  import HttpHead._
  import HttpHeaders._
  import HttpParse._

  val (host, port, path, query) = parseURL(url)
  val parameters = parseParameters(query)


  def withHeader(header: String, value: String): HttpHead = {
    copy(headers = (header -> value) :: headers)
  }

  def singleHeader(name: String): Option[String] = {
    val l = name.toLowerCase
    headers.collectFirst{ case (n, v) if (n == l) => v}
  }

  def multiHeader(name: String): List[String] = {
    val l = name.toLowerCase
    headers.collect{ case (n, v) if (n == l) => v}
  }


  val contentLength: Int = headers.collectFirst{case (ContentLength, l) => l.toInt}.getOrElse(0)


  lazy val cookies: List[Cookie] = multiHeader(CookieHeader).flatMap{Cookie.parseHeader}




  //we should only encode if the string is decoded.
  //To check for that, if we decode an already decoded URL, it should not change (this may not be necessary)
  //the alternative is one big fat honkin ugly regex and knowledge of what characters are allowed where(gross)
  //TODO: this doesn't work, "/test" becomes %2Ftest
  private def getEncodedURL : String = url
  /*{
    if(URLDecoder.decode(url,"UTF-8") == url) {
      URLEncoder.encode(url, "UTF-8")
    }else {
      url
    }
  }*/


  //TODO: optimize
  def bytes : ByteString = {
    val reqString = ByteString(s"${method.name} $getEncodedURL HTTP/$version\r\n")
    if (headers.size > 0) {
      val headerString = ByteString(headers.map{case(k,v) => k + ": " + v}.mkString("\r\n"))
      reqString ++ headerString ++ ByteString("\r\n\r\n")
    } else {
      reqString ++ ByteString("\r\n")
    }
  }
}

object HttpHead {

  import java.net.URI
  def parseParameters(q: String): Map[String, String] = {
    if (q != null && q.length > -1) {
      val params = scala.collection.mutable.HashMap[String, String]()
      val unparsedArgs = q.split('&')

      for (str <- unparsedArgs) {
        val unparsedArg = str.split('=')
        params += unparsedArg(0) -> unparsedArg(1)
      }
      collection.immutable.Map(params.toList: _*)
    } else {
      HashMap.empty[String, String]
    }
  }

  def parseURL(url: String): (String, Int, String, String) = {
    val parsed = new URI(url)
    (parsed.getHost, parsed.getPort, parsed.getPath, parsed.getQuery)
  }


}
