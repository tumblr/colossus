package colossus
package protocols.redis

import akka.util.{ByteString, ByteStringBuilder}
/**
 * The Command class allows us to do basic parsing and building with minimal overhead
 */

case class Command(command: String, args: Seq[ByteString]) {

  def raw = {
    import UnifiedProtocol._
    val size = args.map{_.size}.sum * 2
    val builder = new ByteStringBuilder
    builder.sizeHint(size)
    builder append NUM_ARGS
    builder append ByteString((args.size + 1).toString)
    builder append RN
    builder append ARG_LEN
    builder append ByteString(command.length.toString)
    builder append RN
    builder putBytes command.getBytes
    builder append RN
    args.foreach{ arg =>
      builder append ARG_LEN
      builder append ByteString(arg.size.toString)
      builder append RN
      builder append arg
      builder append RN
    }
    builder.result
  }

  override def toString = command + " " + args.map{_.utf8String}.mkString(" ")
}

object Command {

  def apply(args: String*): Command = {
    Command(args.head, args.tail.map{ByteString(_)})
  }

  def c(rawArgs: Seq[ByteString]): Command = Command(rawArgs.head.utf8String.toUpperCase, rawArgs.tail)

  //can't also be apply because of type erasure
  def c(command: String, args: ByteString*): Command = {
    Command(command, args.toSeq)
  }
}
