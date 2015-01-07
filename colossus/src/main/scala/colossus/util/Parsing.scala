package colossus
package parsing

import core.DataBuffer

import akka.util.{ByteString, ByteStringBuilder}

sealed trait ParseStatus
case object Incomplete extends ParseStatus
case object Complete extends ParseStatus

class ParseException(message: String) extends Exception(message)

case class DataSize(value: Long) extends AnyVal {
  def MB: DataSize = value * 1024 * 1024
  def KB: DataSize = value * 1024
  def bytes = value
}

object DataSize {
  implicit def longToDataSize(l: Long): DataSize = DataSize(l)
}

/** A ParserSizeTracker can wrap a stream parser to ensure that the object beiing parsed doesn't exceed a certain size.
 *
 * The size tracker is not exact.  It simply looks at how many bytes are read
 * off the DataBuffer each time the track method is called.  Since in most
 * cases databuffers are fairly small (128Kb right now for buffers coming out
 * of the event loop), and since the primary purpose for this is to prevent OOM
 * exceptions due to very large requests, the lack of precision isn't a huge
 * issue. 
*/
class ParserSizeTracker(maxSize: Option[DataSize]) {

  //this tracker would probably not work if the DataBuffer ever represents some
  //large/infinite value. we could get around the precision issue by breaking a
  //data buffer into smaller chunks, but that would probably have a performance
  //cost.

  val max = maxSize.map{_.bytes}.getOrElse(Long.MaxValue)

  private var used = 0L

  def reset() {
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

//NOTICE - these classes need to have as little overhead as possible.  No fancy scala stuff here!!!


class UnsizedParseBuffer(terminus: ByteString, includeTerminusInData: Boolean = false, skip: Int = 0) {
  private val data = new ByteStringBuilder()

  private var checkIndex: Int = 0
  private var checkByte: Byte = terminus(0)

  private var skipped = 0

  def addDatum(datum: Byte): ParseStatus = if (skipped < skip) {skipped += 1;Incomplete} else {
    if (datum == checkByte) {
      if (includeTerminusInData) {
        data.putByte(datum)
      }
      if (checkIndex == terminus.size - 1) {
        Complete
      } else {
        checkIndex += 1
        checkByte = terminus(checkIndex)
        Incomplete
      }
    } else {
      if (checkIndex != 0) {
        //we got fooled with a partial terminus, now we have to add what we didn't add before
        if (!includeTerminusInData) {
          (0 until checkIndex).foreach{i => data.putByte(terminus(i))}
        }
        checkIndex = 0
        checkByte = terminus(0)
      }
      data.putByte(datum)
      Incomplete
    }
  }

  def addData(data: DataBuffer): ParseStatus = {
    var last: ParseStatus = Incomplete
    while (data.hasUnreadData && last == Incomplete) {
      last = addDatum(data.next())
    }
    last
  }

  def result: ByteString = data.result()
}

class SizedParseBuffer(val size: Int) {
  private val buffer = new Array[Byte](size)
  private var _received = 0
  def received = _received
  def remaining = size - _received

  def isFinished = size == _received

  def result = ByteString(buffer)

  //len is how many bytes are unread in the buffer
  //returns how many bytes were actually read
  def addData(data: DataBuffer): ParseStatus = {
    val n = math.min(remaining, data.remaining)
    data.takeInto(buffer, _received, n)
    _received += n
    if (remaining == 0) Complete else Incomplete
  }

}

trait IntegerParser{self: UnsizedParseBuffer =>
  def value = java.lang.Integer.parseInt(result.utf8String)
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
  trait Parser[+T] {
    def parse(data: DataBuffer): Option[T]

    def ~[B](b: Parser[B]): Parser[~[T,B]] = {
      val a = this
      new Parser[~[T,B]] {
        var donea: Option[T] = None
        var doneb: Option[B] = None
        def parse(data: DataBuffer): Option[~[T,B]] = {
          if (donea.isEmpty) {
            donea = a.parse(data)
            if (donea.isDefined) {
              parse(data) //need to give b a chance
            } else {
              None
            }                
          } else {
            doneb = b.parse(data)
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
      }
    }
    def andThen[B](b: Parser[B]): Parser[~[T,B]] = this.~(b)

    //combines two parsers but discards the result from the second.  Useful for
    //skipping over data
    def <~[B](b: Parser[B]): Parser[T] = this ~ b >> {_.a}


    //combines two parsers but discards the result from the first.
    def ~>[B](b: Parser[B]): Parser[B] = this ~ b >> {_.b}



    
    def >>[B](f: T => B): Parser[B] = {
      val orig = this
      new Parser[B]{
        def parse(data: DataBuffer) = orig.parse(data).map{r => f(r)}
      }
    }
    def map[B](f: T => B): Parser[B] = >>(f)

    def |>[B](f: T => Parser[B]): Parser[B] = {
      val orig = this
      new Parser[B] {
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
      }
    }
    def flatMap[B](f: T => Parser[B]): Parser[B] = |>(f)
        

  }

  /**
   * Creates a parser that will always return the same value without consuming
   * any data.  Useful when flatMapping parsers
   */
  def const[T](t: T): Parser[T] = new Parser[T] {
    def parse(data: DataBuffer) = Some(t)
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
  def bytes(num: Parser[Long]): Parser[ByteString] = new Parser[ByteString] {
    var buf = new SizedParseBuffer(0) //first one never used 
    var size: Option[Long] = None
    def parse(data: DataBuffer): Option[ByteString] = {
      if (size.isEmpty) {
        size = num.parse(data)
        if (size.isDefined) {
          buf = new SizedParseBuffer(size.get.toInt)//fix toint
          parse(data)
        } else {
          None
        }
      } else if (buf.addData(data) == Complete) {
        val res = Some(buf.result)
        size = None
        res
      } else {
        None
      }
    }
  }

  def bytes(num: Int): Parser[ByteString] = bytes(const(num.toLong))

  /**
   * Keep reading bytes until the terminus is encounted.  This accounts for
   * possible partial terminus in the data.  The terminus is NOT included in
   * the returned value
   */
  def bytesUntil(terminus: ByteString): Parser[ByteString] = new Parser[ByteString] {
    var buf = new UnsizedParseBuffer(terminus)
    def parse(data: DataBuffer): Option[ByteString] = {
      if (buf.addData(data) == Complete) {
        val res = Some(buf.result)
        buf = new UnsizedParseBuffer(terminus)
        res
      } else {
        None
      }
    }
  }

  /** Parse a series of ascii strings seperated by a single-byte delimiter and terminated by a byte
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
  def repeatUntil[T](parser: Parser[T], terminus: Byte): Parser[List[T]] = new Parser[List[T]]{
    var build: List[T] = Nil
    var checkNext = true
    var done = false
    def parse(data: DataBuffer): Option[List[T]] = {
      while (data.hasNext && !done) {
        if (checkNext) {
          checkNext = false
          val b = data.next
          if (b == terminus) {
            done = true
          } else {
            val r = parser.parse(DataBuffer(ByteString(b)))
            if (r.isDefined) {
              build = r.get :: build
              checkNext = true
            }
          }
        } else {
          val r = parser.parse(data)
          if (r.isDefined) {
            build = r.get :: build
            checkNext = true
          }
        }
      }
      if (done) {
        done = false
        checkNext = true
        val res = Some(build)
        build = Nil
        res
      } else {
        None
      }
    }
  }

  //this is just a tuple that allows for cleaner pattern matching
  case class ~[+A,+B](a: A, b: B)

}
    
