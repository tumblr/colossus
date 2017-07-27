package colossus
package protocols.telnet

import core.DataBuffer

import org.scalatest._

import akka.util.ByteString

class TelnetSpec extends WordSpec with MustMatchers{

  def testCmdParser(input: String, expected: TelnetCommand) {
    val p = new TelnetCommandParser
    p.parse(DataBuffer(ByteString(input))) must equal (Some(expected))
  }

  "Telnet Command Parser" must {
    "parse some words" in {
      testCmdParser("A B C\r\n", TelnetCommand(List("A", "B", "C")))
    }

    "parse quoted word" in {
      testCmdParser("A \"B C\" D\r\n", TelnetCommand(List("A", "B C", "D")))
    }

    "parse escaped quote" in {
      testCmdParser("A \\\"B\\\" C\r\n", TelnetCommand(List("A", "\"B\"", "C")))
    }

    "parse escaped quote inside actual quote" in {
      testCmdParser("""A "B \"C\"" C""" + "\r\n", TelnetCommand(List("A", """B "C"""", "C")))
    }

    "parse newline inside quote" in {
      testCmdParser("""A "B""" + "\r\n" + """C" D""" + "\r\n", TelnetCommand(List("A", "B\r\nC", "D")))
    }

    "ignore whitespace in between args" in {
      testCmdParser("A        B C\r\n", TelnetCommand(List("A", "B", "C")))
    }

    "handle empty arg after quotes" in {
      testCmdParser("A \"B\"\r\n", TelnetCommand(List("A", "B")))
    }
  }




}

