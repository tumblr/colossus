package colossus
package protocols

import controller.Codec
import core._
import service._

import akka.util.ByteString

import service._

package object telnet {

  trait Telnet extends Protocol {
    type Request = TelnetCommand
    type Response = TelnetReply
  }

  case class TelnetCommand(args: List[String])

  object TelnetCommand {
    def unapplySeq(cmd: Any): Option[Seq[String]] = cmd match {
      case TelnetCommand(args) => Some(args)
    }
  }


  case class TelnetReply(reply: String) {
    def bytes = {
      val padded = if (reply endsWith "\r\n") reply else reply + "\r\n"
      ByteString(padded)
    }
  }

  class TelnetCommandParser {

    class Builder {
      var args: List[String] = Nil

      //set to true if the last character was a \
      var escaped = false
      var newline = false //set to true on unescaped \r
      var inQuote = false //set to true when we're inside an un-escaped quote
      var argBuilder = new StringBuilder

      def completeArg() {
        if (argBuilder.length > 0) {
          builder.args = builder.argBuilder.toString :: builder.args
          argBuilder = new StringBuilder
        }
      }


      def result = TelnetCommand(args.reverse)
    }

    var builder: Builder = new Builder


    def parse(data: DataBuffer): Option[TelnetCommand] = {
      var line: Option[TelnetCommand] = None
      while (data.hasUnreadData && line.isEmpty) {
        val next = data.next
        if (builder.newline) {
          //done
          builder.completeArg()
          line = Some(builder.result)
          builder = new Builder
        } else if (next == '"') {
          if (builder.escaped) {
            builder.argBuilder.append('"')
            builder.escaped = false
          } else {
            builder.completeArg()
            builder.inQuote = !builder.inQuote //either entering or leaving quote, opposite of whatever state we were already in
          }
        } else if (next == ' ') {
          builder.escaped = false
          if (builder.inQuote) {
            builder.argBuilder.append(' ')
          } else {
            builder.completeArg()
          }
        } else if (next == '\\') {
          if (builder.escaped) {
            builder.argBuilder.append('\\')
            builder.escaped = false
          } else {
            builder.escaped = true
          }
        } else if (next == '\r') {
          builder.escaped = false
          if (builder.inQuote) {
            builder.argBuilder.append('\r')
          } else {
            builder.newline = true
          }
        } else {
          builder.escaped = false
          builder.argBuilder.append(next.toChar)
        }
      }
      line
    }
  }

  //there's a compiler bug that prevents us from doing "extends Telnet[ServerCodec]" :(
  implicit object TelnetServerCodec extends Codec.Server[Telnet] {
    val parser = new TelnetCommandParser

    def decode(data: DataBuffer): Option[TelnetCommand] = parser.parse(data)
    def encode(reply: TelnetReply, buffer: DataOutBuffer){ buffer.write(reply.bytes) }
    def reset(){}

    def apply() = this

    def endOfStream() = None
  }

}
