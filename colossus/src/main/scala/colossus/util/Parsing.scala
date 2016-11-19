package colossus
package parsing

import colossus.metrics.Histogram
import core.DataBuffer

import akka.util.{ByteString, ByteStringBuilder}

import java.nio.ByteBuffer
import scala.util.Try

sealed trait ParseStatus
case object Incomplete extends ParseStatus
case object Complete extends ParseStatus

class ParseException(message: String) extends Exception(message)

case class DataSize(value: Long) extends AnyVal {
  def megabytes = MB
  def MB: DataSize = DataSize(value * 1024 * 1024)
  def kilobytes = KB
  def KB: DataSize = DataSize(value * 1024)
  def bytes = this
}
import DataSize._

object DataSize {

  implicit def longToDataSize(l: Long): DataSize = DataSize(l)
  implicit def intToDataSize(l: Int): DataSize = DataSize(l)

  val StrFormat = "(\\d+) (B|KB|MB)".r

  def apply(str : String) : DataSize = {
    str.toUpperCase match {
      case StrFormat(bytes, "B") => DataSize(bytes.toInt)
      case StrFormat(bytes, "KB") => DataSize(bytes.toInt * 1024)
      case StrFormat(bytes, "MB") => DataSize(bytes.toInt * 1024 * 1024)
      case _ => throw new InvalidDataSizeException(str)
    }
  }

  def unapply(str : String) : Option[DataSize] =  Try(this.apply(str)).toOption

}

class InvalidDataSizeException(format : String) extends Exception(s"The format $format is an invalid Data size.")

/** A ParserSizeTracker can wrap a stream parser to ensure that the object being parsed doesn't exceed a certain size.
 *
 * The size tracker is not exact.  It simply looks at how many bytes are read
 * off the DataBuffer each time the track method is called.  Since in most
 * cases databuffers are fairly small (128Kb right now for buffers coming out
 * of the event loop), and since the primary purpose for this is to prevent OOM
 * exceptions due to very large requests, the lack of precision isn't a huge
 * issue.
*/
class ParserSizeTracker(maxSize: Option[DataSize], histogramOpt: Option[Histogram] = None) {

  //this tracker would probably not work if the DataBuffer ever represents some
  //large/infinite value. we could get around the precision issue by breaking a
  //data buffer into smaller chunks, but that would probably have a performance
  //cost.

  val max = maxSize.map{_.value}.getOrElse(Long.MaxValue)

  private var used = 0L

  def reset() {
    histogramOpt.foreach{_.add(used.toInt)}
    used = 0
  }

  def add(num: Int) {
    used += num
    if (used > max) {
      throw new ParseException(s"Parser data exceeded max size of $max bytes")
    }
  }

  def track[T](buffer: DataBuffer)(op: => Option[T]): Option[T] =  {
    val start = buffer.taken
    val res = op
    add(buffer.taken - start)
    res.foreach{_ => reset()}
    res
  }
}


trait Zero[T] {
  def isZero(t: T): Boolean
}


/** Streaming Parser Combinators
 *
 * === Overview ===
 *
 * A `Parser[T]` is an object that consumes a stream of bytes to produce a result of type `T`.
 *
 * A Combinator is a "higher-order" parser that takes one or more parsers to produce a new parser
 *
 * The Stream parsers are very fast and efficient, but because of this they
 * need to make some tradeoffs.  They are mutable, not thread safe, and in
 * general are designed for network protocols, which tend to have very
 * deterministic grammars.
 *
 * The Parser Rules:
 *
 * 1. A parser must greedily consume the data stream until it produces a result
 * 2. When a parser consumes the last byte necessary to produce a result, it must stop consuming the stream and return the new result while resetting its state
 *
 * === Examples ===
 *
 * Use any parser by itself:{{{
   val parser = bytes(4)
   val data = DataBuffer(ByteString("aaaabbbbccc")
   parser.parse(data) // Some(ByteString(97, 97, 97, 97))
   parser.parse(data) >> {bytes => bytes.utf8String} // Some("bbbb")
   parser.parse(data) // None}}}
 *
 * Combine two parsers{{{
   val parser = bytes(3) ~ bytes(2) >> {case a ~ b => a.ut8String + ":" + b.utf8String}
   parser.parse(DataBuffer(ByteString("abc"))) // None
   parser.parse(DataBuffer(ByteString("defgh"))) // Some("abc:de")}}}

 */
object Combinators {

  class MapParser[A,B](orig: Parser[A], f: A => B) extends Parser[B] {
    def parse(data: DataBuffer) = orig.parse(data) match {
      case Some(r) => Some(f(r))
      case None => None
    }
    override def endOfStream() = orig.endOfStream().map(f)
  }

  class FlatMapParser[A,B](orig: Parser[A], f: A => Parser[B]) extends Parser[B] {
    var mapped: Option[Parser[B]] = None
    def parse(data: DataBuffer) = {
      if (mapped.isDefined) {
        mapped.get.parse(data).map{x => mapped = None;x}
      } else {
        orig.parse(data).map{r => mapped = Some(f(r))}
        if (mapped.isDefined) {
          mapped.get.parse(data).map{x => mapped = None;x}
        } else {
          None
        }
      }
    }

    override def endOfStream() = mapped.flatMap{_.endOfStream}

  }

  class ChainedParser[A,B](first: Parser[A], second: Parser[B]) extends Parser[~[A,B]] {
    var donea: Option[A] = None
    var doneb: Option[B] = None
    def parse(data: DataBuffer): Option[~[A,B]] = {
      if (donea.isEmpty) {
        donea = first.parse(data)
        if (donea.isDefined) {
          parse(data) //need to give b a chance
        } else {
          None
        }
      } else {
        doneb = second.parse(data)
        if (doneb.isDefined) {
          val res = Some(new ~(donea.get, doneb.get))
          donea = None
          doneb = None
          res
        } else {
          None
        }
      }
    }
    override def endOfStream(): Option[~[A,B]] = {
      if (!donea.isDefined) {
        donea = first.endOfStream()
      }
      if (!doneb.isDefined) {
        doneb = second.endOfStream()
      }
      (donea, doneb) match {
        case (Some(ra), Some(rb)) => Some(new ~(ra, rb))
        case _ => None
      }
    }
  }


  trait Parser[+T] {
    def parse(data: DataBuffer): Option[T]

    /**
     * This can be optionally implemented if a parser is able to return a
     * partial value or for situations where the end of the stream signals a
     * complete response
     *
     * Only the unknownLengthBytes parser currently uses this.  It is currently
     * undefined if calling this resets a parser's state.  ">>" also uses it, but not |>
     */
    def endOfStream(): Option[T] = {
      None
    }

    def ~[B](b: Parser[B]): Parser[~[T,B]] = new ChainedParser(this, b)

    def andThen[B](b: Parser[B]): Parser[~[T,B]] = this.~(b)

    //combines two parsers but discards the result from the second.  Useful for
    //skipping over data
    def <~[B](b: Parser[B]): Parser[T] = this ~ b >> {_.a}


    //combines two parsers but discards the result from the first.
    def ~>[B](b: Parser[B]): Parser[B] = this ~ b >> {_.b}

    def >>[B](f: T => B): Parser[B] = new MapParser[T,B](this, f)

    def map[B](f: T => B): Parser[B] = >>(f)

    def |>[B](f: T => Parser[B]): Parser[B] = new FlatMapParser(this, f)

    def flatMap[B](f: T => Parser[B]): Parser[B] = |>(f)


  }

  implicit class ByteArrayOps(val parser: Parser[Array[Byte]]) extends AnyVal {

    def asByteString: Parser[ByteString] = parser >> {d => ByteString(d)}
  }

  /**
   * Creates a parser that will always return the same value without consuming
   * any data.  Useful when flatMapping parsers
   */
  def const[T](t: T): Parser[T] = new Parser[T] {
    val result = Some(t)
    def parse(data: DataBuffer) = result
    override def endOfStream() = result
  }

  def literal(lit: ByteString): Parser[ByteString] = new Parser[ByteString] {
    var index = 0
    val arr = lit.toArray
    def parse(data: DataBuffer) = {
      while(data.hasNext && index < lit.size) {
        if (data.next != arr(index)) {
          throw new ParseException(s"Parsed byte string does not match expected literal")
        }
        index += 1
      }
      if (index == lit.size) {
        index = 0
        Some(lit)
      } else {
        None
      }
    }
  }

  /**
   * Creates a parser that wraps another parser and will throw an exception if
   * more than `size` data is required to parse a single object.  See the
   * ParserSizeTracker for more details.
   */
  def maxSize[T](size: DataSize, parser: Parser[T]): Parser[T] = new Parser[T] {
    val tracker = new ParserSizeTracker(Some(size))
    def parse(data: DataBuffer) = tracker.track(data)(parser.parse(data))
    override def endOfStream() = parser.endOfStream()
  }

  /**
   * parse a single byte
   */
  val byte = new Parser[Byte] {
    def parse(data: DataBuffer) = if (data.hasNext) {
      Some(data.next)
    } else {
      None
    }
  }

  /**
   * read a fixed number bytes, prefixed by a length
   */
  def bytes(num: Parser[Int], maxSize: DataSize, maxInitBufferSize: DataSize): Parser[Array[Byte]] = num |> {s => bytes(s, maxSize, maxInitBufferSize)}


  def bytes(num: Int, maxSize: DataSize, maxInitBufferSize: DataSize): Parser[Array[Byte]] = new Parser[Array[Byte]] {
    if (num < 0 || num > maxSize.bytes.value) {
      throw new ParseException(s"Invalid number $num for bytes parser")
    }

    val builder = new FastArrayBuilder(math.min(num, maxInitBufferSize.bytes.value.toInt), false)

    def parse(data: DataBuffer): Option[Array[Byte]] = {
      val remaining = num - builder.written
      if (data.remaining >= remaining) {
        builder.write(data, remaining)
        Some(builder.complete())
      } else {
        builder.write(data, data.remaining)
        None
      }

    }
  }

  def bytes(num: Parser[Int]): Parser[Array[Byte]] = bytes(num, 10.MB, 1.MB)
  def bytes(num: Int): Parser[Array[Byte]] = bytes(num, 10.MB, 1.MB)

  /**
   * Keep reading bytes until the terminus is encounted.  This accounts for
   * possible partial terminus in the data.  The terminus is NOT included in
   * the returned value
   */
  def bytesUntil(terminus: Array[Byte], includeTerminusInData: Boolean = false, sizeHint: Int = 32): Parser[Array[Byte]] = new Parser[Array[Byte]] {
    private val builder = new FastArrayBuilder(32, true)

    private var terminusPos = 0
    def parse(data: DataBuffer): Option[Array[Byte]] = {
      var res: Option[Array[Byte]] = None
      while (res.isEmpty && data.hasUnreadData) {
        val b = data.next
        if (b == terminus(terminusPos)) {
          terminusPos += 1
          if (terminusPos == terminus.length) {
            res = Some(builder.complete())
            terminusPos = 0
          }
          if (includeTerminusInData) {
            builder.write(b)
          }
        } else {
          if (!includeTerminusInData) {
            var pos = 0
            //need to write the bytes we skipped over so far
            while (pos < terminusPos) {
              builder.write(terminus(pos))
              pos += 1
            }
          }
          terminusPos = 0
          builder.write(b)
        }
      }
      res
    }
  }

  def short: Parser[Short] = bytes(2) >> {b => ByteBuffer.wrap(b).getShort}

  def int: Parser[Int] = bytes(4) >> {b => ByteBuffer.wrap(b).getInt}

  def long: Parser[Long] = bytes(8) >> {b => ByteBuffer.wrap(b).getLong}

  /** Parse a series of ascii strings separated by a single-byte delimiter and terminated by a byte
   *
   */
  def delimitedString(delimiter: Byte, terminus: Byte): Parser[Vector[String]] = new Parser[Vector[String]] {
    var built: Vector[String] = Vector()
    var builder = new StringBuilder
    def parse(data: DataBuffer) = {
      var done = false
      while (data.hasNext && !done) {
        val b = data.next
        if (b == terminus || b == delimiter) {
          built = built :+ builder.toString
          builder = new StringBuilder
          if (b == terminus) {
            done = true
          }
        } else {
          builder.append(b.toChar)
        }
      }
      if (done) {
        val res = Some(built)
        built = Vector()
        res
      } else {
        None
      }
    }
  }



  /** Parse a string until a designated byte is encountered
   *
   * Limited filtering is currently supported, all of which happens during the reading.
   *
   * @param terminus reading will stop when this byte is encountered
   * @param toLower if true any characters in the range A-Z will be lowercased before insertion
   * @param minSize specify a minimum size
   * @param allowWhiteSpace throw a ParseException if any whitespace is encountered before the terminus.  If the terminus is a whitespace character, it will not be counted
   * @param ltrim trim leading whitespace
   *
   */
  def stringUntil(terminus: Byte, toLower: Boolean = false, minSize: Option[Int] = None, allowWhiteSpace: Boolean = true, ltrim: Boolean = false): Parser[String] = new Parser[String] {
    def isWhiteSpace(c: Char) = c == ' ' || c == '\t' || c == '\r' || c == '\n' //maybe regex is faster? (doubtful but worth a shot)
    var build = new StringBuilder
    var leadingWhiteSpace = true //set to false on the first non-whitespace character (for left-trimming)
    def parse(data: DataBuffer) = {
      var done = false
      while (data.hasNext && !done) {
        val b = data.next
        if (b == terminus) {
          minSize.foreach{min => if (build.length < min) {
            throw new ParseException(s"Parsed String ${build.toString} is too small")
          }}
          done = true
        } else {
          val ws = isWhiteSpace(b.toChar)
          if (allowWhiteSpace == false && ws) {
            throw new ParseException(s"Invalid whitespace character '${b.toChar}' ($b) in stream, after '${build.toString}'")
          }
          if (!leadingWhiteSpace || (!ltrim || !ws)) {
            leadingWhiteSpace = false
            if (toLower && b >= 65 && b <= 90) {
              build.append((b + 32).toChar)
            } else {
              build.append(b.toChar)
            }
          }
        }
      }
      if (done) {
        val res = Some(build.toString)
        build = new StringBuilder
        leadingWhiteSpace = true
        res
      } else {
        None
      }
    }
  }

  /**
   * Parses the ASCII representation of an integer, keeps going until the
   * terminus is encountered
   */
  def intUntil(terminus: Byte, base: Int = 10): Parser[Long] = {
    require(base > 0 && base < 17, s"Unsupported integer base $base")
    val numeric_upper = math.min(10, base)
    val alpha_upper = math.max(0, base - 10)
    new Parser[Long] {
      var current: Long = 0
      var negative = false
      var firstByte = true
      var parsedInt = false
      def parse(data: DataBuffer) = {
        var done = false
        while (data.hasUnreadData && !done) {
          val b = data.next
          if (b == terminus && parsedInt) {
            done = true
          } else if (b == '-' && firstByte) {
            negative = true
          } else if (b >= '0' && b <= '0' + numeric_upper) {
            current = (current * base) + (b - '0')
            parsedInt = true
          } else if (base > 10 && b >= 'a'  && b <= 'a' + alpha_upper) {
            current = (current * base) + (b - 'a' + 10)
            parsedInt = true
          } else if (base > 10 && b >= 'A'  && b <= 'A' + alpha_upper) {
            current = (current * base) + (b - 'A' + 10)
            parsedInt = true
          } else {
            throw new ParseException(s"Invalid character '${b.toChar}' ($b) while parsing integer")
          }
          firstByte = false
        }
        if (done) {
          if (negative) {
            current = -current
          }
          val res = Some(current)
          current = 0
          negative = false
          res
        } else {
          None
        }
      }
    }
  }


  /** Parse a pattern multiple times based on a numeric prefix
   *
   * This is useful for any situation where the repeated pattern is prefixed by
   * the number of repetitions, for example `num:[obj1][obj2][obj3]`.  In
   * situations where the pattern doesn't immediately follow the number, you'll
   * have to do it yourself, something like {{{
   intUntil(':') ~ otherParser |> {case num ~ other => repeat(num, patternParser)}}}}
   *
   * @param times parser for the number of times to repeat the pattern
   * @param parser the parser that will parse a single instance of the pattern
   * @return the parsed sequence
   */
  def repeat[T](times: Parser[Long], parser: Parser[T]): Parser[Vector[T]] = new Parser[Vector[T]] {
    var build: Vector[T] = Vector()
    var parsedTimes: Option[Long] = None
    def parse(data: DataBuffer) = {
      if (parsedTimes.isEmpty) {
        parsedTimes = times.parse(data)
        if (parsedTimes.isDefined) {
          parse(data)
        } else {
          None
        }
      } else if (parsedTimes.get > 0) {
        parser.parse(data).foreach{res =>
          build = build :+ res
        }
        if (build.size == parsedTimes.get) {
          val res = Some(build)
          build = Vector()
          res
        } else if (data.hasUnreadData) {
          parse(data)
        } else {
          None
        }
      } else {
        parsedTimes = None
        Some(Vector())
      }
    }
  }

  /** Repeat a pattern a fixed number of times
   *
   * @param times the number of times to parse the pattern
   * @param parser the parser for the pattern
   * @return the parsed sequence
   */
  def repeat[T](times: Long, parser: Parser[T]): Parser[Vector[T]] = repeat(const(times), parser)


  /** Repeatedly parse a pattern until a terminal byte is reached
   *
   * Before calling `parser` this will examine the next byte.  If the byte
   * matches the terminus, it will return the built sequence.  Otherwise it
   * will pass control to `parser` (including the examined byte) until the
   * parser returns a result.
   *
   * Notice that the terminal byte is consumed, so if we have {{{
   val parser = repeatUntil(bytes(2), ':')
   parser.parse(DataBuffer(ByteString("aabbcc:ddee")))
   }}}
   * the bytes remaining in the buffer after parsing are just `ddee`.
   *
   * @param parser the parser repeat
   * @param terminus the byte to singal to stop repeating
   * @return the parsed sequence
   */
  def repeatUntil[T](parser: Parser[T], terminus: Byte): Parser[Vector[T]] = new Parser[Vector[T]]{
    var build: Vector[T] = Vector()
    var checkNext = true
    var done = false
    def parse(data: DataBuffer): Option[Vector[T]] = {
      while (data.hasNext && !done) {
        if (checkNext) {
          checkNext = false
          val b = data.next
          if (b == terminus) {
            done = true
          } else {
            val r = parser.parse(DataBuffer(ByteString(b)))
            if (r.isDefined) {
              build = build :+ r.get
              checkNext = true
            }
          }
        } else {
          val r = parser.parse(data)
          if (r.isDefined) {
            build = build :+ r.get
            checkNext = true
          }
        }
      }
      if (done) {
        done = false
        checkNext = true
        val res = Some(build)
        build = Vector()
        res
      } else {
        None
      }
    }
  }

  /**
   * uses the parser but then immediately resets the databuffer to its position
   * before the parser was invoked.  This is useful is you need to parse
   * incoming data, but leave the data itself untouched, or for look-ahead
   * parsing, but be aware of the performance implications of possibly reading
   * the same segment of data multiple times.
   */
   /*
    NOTE - this is commented out because right now in all cases we need to know
    how much data was peeked, so instead we're using peek on the databuffer and
    regular parsers inside of the peek,
  def peek[T](p: Parser[T]): Parser[T] = new Parser[(T] {
    def parse(data: DataBuffer): Option[T] = data.peek{buf => p.parse(data)}
  }
  */

  /**
   * creates a parser that will skip over n bytes.  You generally only want to do this inside a peek parser
   */
  def skip[T](n: Int): Parser[Unit] = new Parser[Unit] {
    private var left = n
    private def reset() {
      left = n
    }
    def parse(data: DataBuffer): Option[Unit] = {
      if (data.remaining >= left) {
        data.skip(left)
        reset()
        Some(Unit)
      } else {
        left -= data.remaining
        data.skipAll
        None
      }
    }
  }

  /** Read in an unknown number of bytes, ended only when endOfStream is called
   *
   * be aware this parser has no max size and will read in data forever if endOfStream is never called
   */
  def bytesUntilEOS: Parser[ByteString] = new Parser[ByteString] {
    var builder = new ByteStringBuilder
    def parse(data: DataBuffer) = {
      builder.putBytes(data.takeAll)
      None
    }

    override def endOfStream() = {
      val res = builder.result
      builder = new ByteStringBuilder //probably not required, we should probably formalize that this parser will never be used again once this is called
      Some(res)
    }
  }

  /**
   * Create a parser that will repeat the given parser forever until
   * `endOfStream()` is called.  The results from each call to the given parser
   * are accumulated and returned at the end of the stream.
   */
  def repeatUntilEOS[T](parser: Parser[T]): Parser[Seq[T]] = new Parser[Seq[T]] {
    var build = collection.mutable.ArrayBuffer[T]()
    def parse(data: DataBuffer) = {
      while (data.hasNext) {
        parser.parse(data).foreach{t =>
          build += t
        }
      }
      None
    }

    override def endOfStream() = {
      val res = Some(build)
      build = collection.mutable.ArrayBuffer[T]()
      res
    }
  }





  /**
   * Repeat a parser, accumulating the results until the value returned by the
   * parser matches the type's Zero value.  This can be used, for example, to
   * keep parsing lines of data until an empty line is encountered
   */
  class RepeatZeroParser[T : scala.reflect.ClassTag](parser: Parser[T])(implicit zero: Zero[T]) extends Parser[Array[T]] {
    val build = new java.util.LinkedList[T]()

    def parse(data: DataBuffer): Option[Array[T]] = {
      var done = false
      while (data.hasUnreadData && !done) {
        parser.parse(data) match {
          case Some(res) => if (!zero.isZero(res)) {
            build.add(res)
          } else {
            done = true
          }
          case None => {}
        }
      }
      if (done) {
        val h = new Array[T](build.size)
        val it = build.iterator
        var i = 0
        while (it.hasNext) {
          h(i) = it.next
          i += 1
        }
        build.clear()
        Some(h)
      } else {
        None
      }
    }
  }

  class FoldZeroParser[T, U](parser: Parser[T], init: => U)(folder: (T, U) => U)(implicit zero: Zero[T]) extends Parser[U] {

    var current: U = init

    def parse(data: DataBuffer): Option[U] = {
      var res: Option[U] = None
      while (data.hasUnreadData && res == None) {
        parser.parse(data) match {
          case Some(v) => if (!zero.isZero(v)) {
            current = folder(v, current)
          } else {
            res = Some(current)
            current = init
          }
          case None => {}
        }
      }
      res
    }
  }


  /**
   * Repeat using a parser until it returns a zero value.  An array of non-zero
   * values is returned
   */
  def repeatZero[T : scala.reflect.ClassTag](parser: Parser[T])(implicit zero: Zero[T]) = new RepeatZeroParser(parser)

  def foldZero[T, U](parser: Parser[T], init: => U)(folder: (T, U) => U)(implicit zero: Zero[T]) = new FoldZeroParser(parser, init)(folder)

  /**
   * A very fast dynamically growable array builder.  Do not be tempted to
   * replace this with any out-of-the-box Java/Scala class.  This is faster.
   */
  trait FastArrayBuilding {

    def initSize: Int
    def shrinkOnComplete: Boolean

    //TODO : This class is somewhat similar to the DynamicOutBuffer, maybe
    //there's a way to avoid duplicated logic

    private var build: Array[Byte] = new Array[Byte](initSize)

    private var writePos = 0

    def written = writePos

    @inline final private def grow() {
      val nb = new Array[Byte](build.length * 2)
      System.arraycopy(build, 0, nb, 0, build.length)
      build = nb
    }

    @inline final def write(b: Byte) {
      if (writePos == build.length) {
        grow()
      }
      build(writePos) = b
      writePos += 1
    }

    def write(buffer: DataBuffer, bytes: Int) {
      while (writePos + bytes > build.length) {
        grow()
      }
      buffer.takeInto(build, writePos, bytes)
      writePos += bytes
    }

    def write(bytes: Array[Byte]) {
      while (writePos + bytes.length > build.length) {
        grow()
      }
      System.arraycopy(bytes, 0, build, writePos, bytes.length)
      writePos += bytes.length
    }



    def complete(): Array[Byte] = {
      val res = new Array[Byte](writePos)
      System.arraycopy(build, 0, res, 0, writePos)
      writePos = 0
      if (shrinkOnComplete && build.length > initSize) {
        build = new Array(initSize)
      }
      res
    }
  }
  class FastArrayBuilder(val initSize: Int, val shrinkOnComplete: Boolean = false) extends FastArrayBuilding



  /**
   * Parse a single line of data.  A "line" is terminated by `\r\n`.
   *
   * This is quite possibly the fastest line parser in existence.  While this is
   * basically a specialized version of the bytesUntil parser, it is
   * significantly faster.  Part of the speedup is simply from basically
   * including the functionality of the MapParser, which avoids a bunch of
   * function calls.  I believe the rest of the speedup is due to the fact that
   * comparing the next byte to a constant vs an array member is significantly
   * faster.  I have made several attempts to get the bytesUntil parser as fast
   * as this one to no avail.
   */
  class LineParser[T](constructor: Array[Byte] => T, includeNewline: Boolean = false, internalBufferBaseSize: Int = 100) extends Parser[T] with FastArrayBuilding {
    private val CR    = '\r'.toByte
    private val LF    = '\n'.toByte

    def initSize = internalBufferBaseSize
    def shrinkOnComplete = false

    var scanByte = CR

    private final def checkLineFeed(buffer: DataBuffer) : T = {
      val b = buffer.data.get
      if (b == LF) {
        if (includeNewline) {
          write(CR)
          write(LF)
        }
        scanByte = CR
        constructor(complete())
      } else {
        throw new ParseException("Malformed newline, expected \\r, got '$b'")
      }
    }

    def parse(buffer: DataBuffer): Option[T] = {
      var res: Option[T] = None
      if (scanByte == LF && buffer.hasUnreadData) {
        res = Some(checkLineFeed(buffer))
      }
      while (buffer.hasUnreadData && res == None) {
        val byte = buffer.data.get
        if (byte == CR ) {
          if (buffer.hasUnreadData) {
            res = Some(checkLineFeed(buffer))
          } else {
            //this would only happen if the \n is in the next packet/buffer,
            //very rare but it can happen, but we can't complete until we've read it in
            scanByte = LF
          }
        } else {
          write(byte)
        }
      }
      res
    }



  }
  def line: Parser[Array[Byte]] = line(false)

  def line(includeNewline: Boolean): Parser[Array[Byte]] = new LineParser(x => x, includeNewline)

  def line[T](constructor: Array[Byte] => T, includeNewLine: Boolean) : Parser[T] = new LineParser(constructor, includeNewLine)


  //this is just a tuple that allows for cleaner pattern matching
  case class ~[+A,+B](a: A, b: B)

}

