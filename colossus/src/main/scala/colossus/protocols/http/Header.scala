package colossus
package protocols.http

import akka.util.ByteString
import colossus.protocols.http.Connection.{Close, KeepAlive}
import com.github.nscala_time.time.Imports._

import scala.collection.immutable.HashMap
import parsing.ParseException


sealed abstract class HttpMethod(val name: String) {
  val bytes: Array[Byte] = name.getBytes("UTF-8")
}

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

  def apply(str: String): HttpMethod = {
    val ucase = str.toUpperCase
    def loop(remain: List[HttpMethod]): HttpMethod = remain match {
      case head :: tail => if (head.name == ucase) head else loop(tail)
      case Nil => throw new ParseException(s"Invalid http method $str")
    }
    loop(methods)    
  }
}

sealed abstract class HttpVersion(versionString: String) {
  override def toString = versionString
  val messageBytes = ByteString("HTTP/" + versionString)
  val messageArr = messageBytes.toArray
}
object HttpVersion {
  case object `1.0` extends HttpVersion("1.0")
  case object `1.1` extends HttpVersion("1.1")

  def apply(str: String): HttpVersion = {
    if (str == "HTTP/1.1") `1.1` else if (str=="HTTP/1.0") `1.0` else throw new ParseException(s"Invalid http version '$str'")
  }
}


trait HttpHeader {
  def key: String
  def value: String
  def encoded: Array[Byte]

  override def equals(that: Any): Boolean = that match {
    case that: HttpHeader => this.key == that.key && this.value == that.value
    case other => false
  }

  override def hashCode = (key + value).hashCode

  override def toString = s"($key,$value)"

}

//generally created when encoding http responses
class EncodedHeader(val encoded: Array[Byte], keyLength: Int, valueStart: Int) extends HttpHeader {
  lazy val key = new String(encoded.take(keyLength))
  lazy val value = new String(encoded.drop(valueStart).dropRight(HttpHeader.NEWLINE.length))
}

//generally created when parsing http requests
class DecodedHeader(val key: String, val value: String) extends HttpHeader {
  lazy val encoded = (key + HttpHeader.DELIM + value + HttpHeader.NEWLINE).getBytes("UTF-8")

  def toEncodedHeader = new EncodedHeader(encoded, key.length, key.length + HttpHeader.DELIM.length)
}


object HttpHeader {
  val DELIM = ": "
  val NEWLINE = "\r\n"


  def apply(key: String, value: String): HttpHeader = (new DecodedHeader(key, value)).toEncodedHeader

}
    
class HttpHeaders(private val headers: Array[HttpHeader]) {
  def firstValue(name: String): Option[String] = {
    val l = name.toLowerCase
    headers.collectFirst{ case x if (x.key == l) => x.value }
  }

  def allValues(name: String): Seq[String] = {
    val l = name.toLowerCase
    headers.collect{ case x if (x.key == l) => x.value }
  }

  /** Returns the value of the content-length header, if it exists.
   * 
   * Be aware that lacking this header may or may not be a valid request,
   * depending if the "transfer-encoding" header is set to "chunked"
   */
  def contentLength: Option[Int] = firstValue(HttpHeaders.ContentLength).map{_.toInt}

  def transferEncoding : TransferEncoding = firstValue(HttpHeaders.TransferEncoding).flatMap(TransferEncoding.unapply).getOrElse(TransferEncoding.Identity)

  def connection: Option[Connection] = firstValue(HttpHeaders.Connection).flatMap(Connection.unapply)

  def + (kv: (String, String)) = new HttpHeaders(headers :+ HttpHeader(kv._1, kv._2))

  def size = headers.size

  def toSeq : Seq[HttpHeader] = headers

  def encode(buffer: core.DataOutBuffer) {
    var i = 0
    while (i < headers.size) {
      buffer.write(headers(i).encoded)
      i += 1
    }

  }

  override def equals(that: Any): Boolean = that match {
    case that: HttpHeaders => this.toSeq.toSet == that.toSeq.toSet
    case other => false
  }

  override def toString = "[" + headers.map{_.toString}.mkString(" ") + "]"

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
  val Connection        = "connection"
  val ContentLength     = "content-length"
  val ContentType       = "content-type"
  val CookieHeader      = "cookie"
  val SetCookie         = "set-cookie"
  val TransferEncoding  = "transfer-encoding"

  def apply(hdrs: HttpHeader*) : HttpHeaders = new HttpHeaders(hdrs.toArray)

  val Empty = new HttpHeaders(Array())
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

sealed trait TransferEncoding {
  def value : String
}

object TransferEncoding {
  case object Identity extends TransferEncoding {
    val value = "identity"
  }

  case object Chunked extends TransferEncoding {
    val value = "chunked"
  }

  private val all = Seq(Identity, Chunked)
  def unapply(str : String) : Option[TransferEncoding] = {
    all.find(_.value == str.toLowerCase)
  }
}

sealed trait ContentEncoding {
  def value: String
}
object ContentEncoding {

  case object Identity extends ContentEncoding {
    val value = "identity"
  }

  case object Gzip extends ContentEncoding {
    val value = "gzip"
  }

  case object Deflate extends ContentEncoding {
    val value = "deflate"
  }

  case object Compressed extends ContentEncoding {
    val value = "compressed"
  }

  private val all = Seq(Gzip, Deflate, Compressed, Identity)
  def unapply(str : String) : Option[ContentEncoding] = {
    all.find(_.value == str.toLowerCase)
  }
}

sealed trait Connection {
  def value: String
}
object Connection {
  case object KeepAlive extends Connection {
    val value = "keep-alive"
  }
  case object Close extends Connection {
    val value = "close"
  }

  private val all = Seq(Close, KeepAlive)
  def unapply(str : String) : Option[Connection] = {
    all.find(_.value == str)
  }
}



case class QueryParameters(parameters: Seq[(String, String)]) extends AnyVal{

  def apply(key: String) = getFirst(key).get

  /**
   * Get the value of a query string parameter when only at most one value is
   * expected.  If there are multiple instances of the parameter then only the
   * value of the first is returned
   **/
  def getFirst(key: String): Option[String] = parameters.collectFirst{case (k,v) if k == key => v}

  /**
   * Get the values of all instances of key
   *
   * This is for urls like http://foo.com?bar=val1&bar=val2&bar=val3, which is a valid url
   */
  def getAll(key: String) : Seq[String] = parameters.collect{case (k,v) if k == key => v}

  /**
   * return true if at least one parameter's key matches the given key
   */
  def contains(key: String): Boolean = parameters.exists{case (k,v) => k == key}

}

class DateHeader extends HttpHeader {
  import java.util.Date
  import java.text.SimpleDateFormat

  private val formatter = new SimpleDateFormat("EEE, MMM d yyyy HH:MM:ss z")
  
  private def generate = HttpHeader("Date", formatter.format(new Date()))
  private var lastDate = generate
  private var lastTime = System.currentTimeMillis

  def key = lastDate.key
  def value = lastDate.value

  def encoded = {
    val t = System.currentTimeMillis
    if (t > lastTime + 1000) {
      lastDate = generate
      lastTime = t
    }
    lastDate.encoded
  }

}


class HttpBody(private val body: Array[Byte], val contentType : Option[HttpHeader] = None)  {

  def size = body.size

  def encode(buffer: core.DataOutBuffer) {
    if (size > 0) buffer.write(body)
  }

  def bytes: ByteString = ByteString(body)

  override def equals(that: Any) = that match {
    case that: HttpBody => that.bytes == this.bytes
    case other => false
  }

  override def toString = bytes.utf8String

}

trait HttpBodyEncoder[T] {
  def encode(data: T): HttpBody
}

trait HttpBodyEncoders {
  implicit object ByteStringEncoder extends HttpBodyEncoder[ByteString] {
    def encode(data: ByteString): HttpBody = new HttpBody(data.toArray)
  }

  implicit object StringEncoder extends HttpBodyEncoder[String] {
    val ctype = HttpHeader("Content-Type", "text/plain")
    def encode(data: String) : HttpBody = new HttpBody(data.getBytes("UTF-8"), Some(ctype))
  }

  implicit object IdentityEncoder extends HttpBodyEncoder[HttpBody] {
    def encode(b: HttpBody) = b
  }
}

object HttpBody extends HttpBodyEncoders {
  
  val NoBody = HttpBody("")
  
  def apply[T](data: T)(implicit encoder: HttpBodyEncoder[T]): HttpBody = encoder.encode(data)

}
