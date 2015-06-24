package colossus

import core.DataBuffer

import parsing._
import Combinators._

import org.scalatest._

import akka.util.ByteString


class CombinatorSuite extends WordSpec with MustMatchers{

  def data(str: String) = DataBuffer(ByteString(str))
  def bstr(str: String) = ByteString(str)

  "parsers" must {
    "bytes" in {
      val d = data("abcdefg")
      val parser = bytes(3)
      parser.parse(d) must equal (Some(bstr("abc")))
      parser.parse(d) must equal (Some(bstr("def")))
      parser.parse(d) must equal (None)
    }
    "bytes holds state" in {
      val parser = bytes(3)
      parser.parse(data("ab")) must equal (None)
      parser.parse(data("cd")) must equal (Some(bstr("abc")))
    }
    "bytes with parser" in {
      val parser = bytes(intUntil(':'))
      val d = data("12:abcdefghijklmn")
      parser.parse(d) must equal (Some(bstr("abcdefghijkl")))
    }
    "const is const" in {
      val d = data("abcdefg")
      val parser = const(1)
      parser.parse(d) must equal (Some(1))
      parser.parse(d) must equal (Some(1))
      parser.endOfStream() must equal (Some(1))
    }
    "repeat" in {
      val parser = repeat(3, bytes(2))
      val d = data("abcdefgh")
      parser.parse(d) must equal (Some(Vector(bstr("ab"), bstr("cd"), bstr("ef"))))
    }
    "repeatUntil" in {
      val parser = repeatUntil(bytes(2), '!')
      val d = data("abcdef!")
      parser.parse(d) must equal (Some(Vector(bstr("ab"), bstr("cd"), bstr("ef"))))
    }
    "intUntil" in {
      val parser = intUntil('!')
      val d = data("109834!")
      parser.parse(d) must equal (Some(109834L))
    }
    "intUntil with negative" in {
      val parser = intUntil('!')
      val d = data("-109834!")
      parser.parse(d) must equal (Some(-109834L))
    }
    "intUntil fail on invalid integer" in {
      val invalid = Seq("-!", "0-!", "--!", "-1-1!")
      invalid.foreach { x =>
        intercept[ParseException] {
          val parser = intUntil('!')
          val d = data(x)
          parser.parse(d)
        }
      }
    }
    "intUntil with hex base" in {
      val parser = intUntil('!', 16)
      parser.parse(data("3A3f!")) must equal(Some(0x3A3F))
    }
    "intUntil fails on invalid letter in hex" in {
      intercept[ParseException] {
        val parser = intUntil('!')
        parser.parse(data("32G"))
      }
    }
    "intUntil fails on invalid base" in {
      intercept[Exception] {
        val parser = intUntil('!', 17)
      }
      intercept[Exception] {
        val parser = intUntil('!', 0)
      }

    }
    "literal" in {
      val parser = literal(ByteString("hello"))
      val d = data("helloasdf")
      parser.parse(d) must equal(Some(ByteString("hello")))

      intercept[ParseException] {
        parser.parse(d)
      }
    }
    "bytesUntil" in {
      val parser = bytesUntil(ByteString("iii"))
      val d = data("xxxiixxxiiixxx")
      parser.parse(d) must equal(Some(ByteString("xxxiixxx")))
    }
    "stringUntil - whitespace" in {
      val parser = stringUntil('4')
      val d = data(" abc DeF ghi 4ghghg")
      parser.parse(d) must equal(Some(" abc DeF ghi "))
    }
    "stringUntil reject whitespace" in {
      val parser = stringUntil('4', allowWhiteSpace = false)
      val d = data(" abc DeF ghi 4ghghg")
      intercept[ParseException] {
        parser.parse(d)
      }
    }
    "stringUntil ltrim" in {
      val parser = stringUntil('4', ltrim = true)
      val d = data(" abc DeF ghi 4ghghg")
      parser.parse(d) must equal(Some("abc DeF ghi "))
    }
    "stringUntil lowercase" in {
      val parser = stringUntil('4', toLower = true)
      val d = data(" abc DeF ghi 4ghghg")
      parser.parse(d) must equal(Some(" abc def ghi "))
    }

    "short" in {
      val parser = short
      val data = DataBuffer(ByteString(0x1D, 0x76, 0x82, 0x71, 0x55, 0xC2, 0x12, 0x00)) //6 extra bytes
      parser.parse(data) must equal(Some(7542))
      data.remaining must equal(6)
    }

    "int" in {
      val parser = int
      val data = DataBuffer(ByteString(0x00, 0x0B, 0x82, 0x71, 0x55, 0xC2, 0x12, 0x00)) //4 extra bytes
      parser.parse(data) must equal(Some(754289))
      data.remaining must equal(4)
    }

    "long" in {
      val parser = long
      val data = DataBuffer(ByteString(0x01, 0x0B, 0xF4, 0x3A, 0xE2, 0x64, 0x13, 0xBE))
      parser.parse(data) must equal(Some(75422352525235134L))
      data.remaining must equal(0)
    }

    "bytesUntilEOS" in {
      val parser = bytesUntilEOS
      val d1 = ByteString(1, 2, 3)
      val d2 = ByteString(4, 5, 6)
      parser.parse(DataBuffer(d1)) must equal(None)
      parser.parse(DataBuffer(d2)) must equal(None)
      parser.endOfStream() must equal(Some(ByteString(1, 2, 3, 4, 5, 6)))
    }

    "bytesUntilEOS as map subject" in {
      val parser = bytesUntilEOS >> {bytes => bytes.size}
      val d1 = ByteString(1, 2, 3)
      val d2 = ByteString(4, 5, 6)
      parser.parse(DataBuffer(d1)) must equal(None)
      parser.parse(DataBuffer(d2)) must equal(None)
      parser.endOfStream() must equal(Some(6))
    }

    "bytesUntilEOS as flatMap object" in {
      //notice that using this parser as the subject makes no sense
      val parser = bytes(3) |> {bytes => bytesUntilEOS}
      val d1 = ByteString(1, 2, 3)
      val d2 = ByteString(4, 5, 6)
      parser.parse(DataBuffer(d1)) must equal(None)
      parser.parse(DataBuffer(d2)) must equal(None)
      parser.endOfStream() must equal(Some(ByteString(4,5,6)))
    }
      


  }

  "combinators" must {
    "combine basic" in {
      val parser = bytes(3) ~ bytes(4)
      val d = data("123abcd")
      parser.parse(d) must equal (Some(new ~(bstr("123"), bstr("abcd"))))
    }

    "combine more complex parsers" in {
      val parser = bytes(3) ~ repeat(2, bytes(3)) ~ bytes(4)
      val d = data("123x00x11abcd")
      val expected = new ~(new ~(bstr("123"), Vector(bstr("x00"), bstr("x11"))), bstr("abcd"))
      parser.parse(d) must equal(Some(expected))
    }

    "map" in {
      val parser = bytes(3) ~ bytes(4) >> {case a ~ b => (a.utf8String, b.utf8String)}
      val d = data("123abcd")
      parser.parse(d) must equal (Some(("123", "abcd")))
    }

    "repeat - fixed" in {
      val parser = repeat(3, bytes(2))
      val d = data("aabbccdd")
      parser.parse(d) must equal (Some(Vector(ByteString("aa"), ByteString("bb"), ByteString("cc"))))
    }
    "repeat - dynamic" in {
      val parser = repeat(intUntil(':'), bytes(2))
      val d = data("3:aabbccdd")
      parser.parse(d) must equal (Some(Vector(ByteString("aa"), ByteString("bb"), ByteString("cc"))))
    }

    "repeatUntilEOS" in {
      val parser = repeatUntilEOS(bytes(2))
      val d1 = data("aabb")
      val d2 = data("cc")
      parser.parse(d1) must equal(None)
      parser.parse(d2) must equal(None)
      parser.endOfStream() must equal(Some(Vector(ByteString("aa"), ByteString("bb"), ByteString("cc"))))
    }
  }

}
      

