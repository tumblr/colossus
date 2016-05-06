package colossus
package protocols.redis

import akka.util.{ByteString, ByteStringBuilder}
import parsing._
import core.DataBuffer
import Combinators._
import DataSize._

object RedisCommandParser {

  val DefaultMaxSize: DataSize = 1.MB

  def apply(size: DataSize = DefaultMaxSize) = maxSize(size, command)

  def command: Parser[Command] = byte |> {
    case '*' => unified
    case n => line >> {data => Command((ByteString(n) ++ ByteString(data)).utf8String.split(" "):_*)}
  }

  def unified: Parser[Command] = repeat(argNum, arg) >> {args => Command.c(args)}
  def arg = bytes(argLength).map{b => ByteString(b)} <~ bytes(2)
  def argLength = literal($_BYTE) ~> intUntil('\r').map{_.toInt} <~ byte
  def argNum = intUntil('\r') <~ byte


  val $_BYTE = ByteString("$")
}


