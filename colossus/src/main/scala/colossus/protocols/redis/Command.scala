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

  def isReadCommand = UnifiedProtocol.readCommands contains command
}

object Commands {
  import UnifiedProtocol._

  object Get {
    def apply(key: ByteString) = Command.c(CMD_GET, key)
    def unapply(c: Command): Option[ByteString] = if (c.command == CMD_GET && c.args.size == 1) Some(c.args.head) else None
  }
  object Set {
    def apply(key: ByteString, value: ByteString) = Command.c(CMD_SET, key, value)
    def unapply(c: Command): Option[(ByteString, ByteString)] = if (c.command == CMD_SET && c.args.size == 2) Some((c.args(0), c.args(1))) else None
  }
  object Setnx {
    def apply(key: ByteString, value: ByteString) = Command.c(CMD_SETNX, key, value)
  }
  object Setex {
    def apply(key: ByteString, value: ByteString) = Command.c(CMD_SETEX, key, value)
  }
  object Strlen {
    def apply(key: ByteString) = Command.c(CMD_STRLEN, key)
  }
  object ZAdd {
    def apply(key: ByteString, score: ByteString, value: ByteString) = Command.c(CMD_ZADD, key, score, value)
  }
  object ZCount {
    def apply(key: ByteString, startScore: ByteString, endScore: ByteString) = Command.c(CMD_ZCOUNT, key, startScore, endScore)
  }
  object ZRange {
    def apply(key: ByteString, startIndex: ByteString, endIndex: ByteString) = Command.c(CMD_ZRANGE, key, startIndex, endIndex)
  }
  object ZRangeByScore {
    def apply(key: ByteString, startScore: ByteString, endScore: ByteString): Command = {
      Command.c(CMD_ZRANGEBYSCORE, key, startScore, endScore)
    }
    def apply(key: ByteString, startScore: ByteString, endScore: ByteString, offset: ByteString, count: ByteString): Command = {
      Command.c(CMD_ZRANGEBYSCORE, key, startScore, endScore, ByteString("LIMIT"), offset, count)
    }
  }
  object ZRevRangeByScore {
    def apply(key: ByteString, startScore: ByteString, endScore: ByteString): Command = {
      Command.c(CMD_ZREVRANGEBYSCORE, key, startScore, endScore)
    }
    def apply(key: ByteString, startScore: ByteString, endScore: ByteString, offset: ByteString, count: ByteString): Command = {
      Command.c(CMD_ZREVRANGEBYSCORE, key, startScore, endScore, ByteString("LIMIT"), offset, count)
    }
  }
  object ZRemRangeByRank {
    def apply(key: ByteString, startScore: ByteString, endScore: ByteString) = Command.c(CMD_ZREMRANGEBYRANK, key, startScore, endScore)
  }
  object ZRemRangeByScore {
    def apply(key: ByteString, startIndex: ByteString, endIndex: ByteString) = Command.c(CMD_ZREMRANGEBYSCORE, key, startIndex, endIndex)
  }
  object ZRem {
    def apply(key: ByteString, value: ByteString) = Command.c(CMD_ZREM, key, value)
  }
  object HGet {
    def apply(key: ByteString, field: ByteString) = Command.c(CMD_HGET, key, field)
  }
  object HSet {
    def apply(key: ByteString, field: ByteString, value: ByteString) = Command.c(CMD_HSET, key, field, value)
  }
  object HMGet {
    def apply(key: ByteString, fields: ByteString*) = Command.c(CMD_HMGET, key  +: fields:_*)
  }
  object Del {
    def apply(key: ByteString) = Command.c(CMD_DEL, key)
    def unapply(c: Command): Option[ByteString] = if (c.command == CMD_DEL && c.args.size == 1) Some(c.args.head) else None
  }
  object HDel {
    def apply(key: ByteString, fields: ByteString*) = Command.c(CMD_HDEL, key +: fields:_*)
  }

}

object Command {
  
  import UnifiedProtocol._

  def apply(args: String*): Command = {
    Command(args.head, args.tail.toSeq.map{ByteString(_)})
  }

  def c(rawArgs: Seq[ByteString]): Command = Command(rawArgs.head.utf8String.toUpperCase, rawArgs.tail)

  //can't also be apply because of type erasure
  def c(command: String, args: ByteString*): Command = {
    Command(command, args.toSeq)
  }

}

//All commands are put into this Trie to make searching commands O(log n)
//instead of O(n).  Actually makes a big deal since we have to do this for
//every command
object CommandTrie {

  def expand(letter: Byte): Seq[Byte] = if (letter >= 'a' && letter <= 'z') {
    Seq(letter, letter.toChar.toString.toUpperCase.toCharArray()(0).toByte)
  } else if (letter >='A' && letter <= 'Z') {
    Seq(letter, letter.toChar.toString.toLowerCase.toCharArray()(0).toByte)
  } else {
    Seq(letter) //mostly for terminus
  }

  def apply(commands: String*): ImmutableNode = {
    val n = new NodeBuilder
    commands.foreach{n.add}
    n.toNode
  }


  trait Node[T <: Node[T]] {self: T =>
    val letters: Seq[Byte] //multiple letters for case insensitive matching!
    val children: Seq[T]

    def childFor(letter: Byte): Option[T] = children.find{_.letters contains letter}

    protected def containsHelper(cmd: Seq[Byte]): Boolean = cmd.headOption match {
      case Some(letter) => childFor(letter).map{_.containsHelper(cmd.tail)}.getOrElse(false)
      case None => children.size == 0
    }

    def contains(cmd: ByteString): Boolean = containsHelper(cmd :+ 0.toByte)

    def contains(s: String): Boolean = containsHelper(s.toCharArray.map{_.toByte} :+ 0.toByte)

    def traverseTo(cmd: Seq[Byte]): (Seq[Byte], T) = cmd.headOption match {
      case Some(letter) => childFor(letter).map{_.traverseTo(cmd.tail)}.getOrElse((cmd, this))
      case None => (Nil, this)
    }
  }

  case class ImmutableNode(letters: Seq[Byte], children: Seq[ImmutableNode]) extends Node[ImmutableNode]{
    type T = ImmutableNode
    override def toString = letters.map{_.toChar}.mkString(",") + "[" + children.map{_.toString}.mkString(" ") + "]"
  }

  class NodeBuilder(val letters: Seq[Byte]) extends Node[NodeBuilder]{
    def this() = this(Seq(0.toByte))
      
    val children = collection.mutable.ArrayBuffer[NodeBuilder]()
    def addChild(child: NodeBuilder) {
      children += child
    }

    def toNode: ImmutableNode = ImmutableNode(letters, children.toSeq.map{_.toNode})

    def build: Node[ImmutableNode] = toNode

    def add(word: Seq[Byte]) {
      traverseTo(word) match {
        case (Nil, node) => {}
        case (list, node) => {
          val child = new NodeBuilder(expand(list.head))
          child.add(list.tail)
          node.addChild(child)
        }
      }
    }

    /**
     * note - we add a 0-byte at the end so we can distinguish between SET and
     * SETEX while still not allowing partial matches
     */
    def add(command: String) {
      add(ByteString(command) :+ 0.toByte)
    }


  }
}
