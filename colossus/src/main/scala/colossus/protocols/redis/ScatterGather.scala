package colossus
package protocols.redis

import akka.util.ByteString

//TODO: Unify with rediscover, radish
trait CommandHasher[T] {
  def shardFor(key: ByteString): T
}

trait ScatterGather[T] {
  /**
   * Scatters a command.  Notice the return type is not a Map becuase the order
   * is important and must match the order of replies in gather
   */
  def scatter: Seq[(T, Command)]
  def gather(replies: Seq[Reply]): Reply
}

object ScatterGather {
  def scatterGrouped[T](args: Seq[ByteString], hasher: CommandHasher[T], cmd: ByteString, groupSize: Int): (Map[Int, Seq[Int]], Seq[(T, Command)]) = {
    val shardIndices = collection.mutable.Map[Int, Seq[Int]]()
    val shardCommands = collection.mutable.Map[T, collection.mutable.ArrayBuffer[ByteString]]()
    val indexBuffer = collection.mutable.Map[T, collection.mutable.ArrayBuffer[Int]]()
    var argIndex = 0
    var i = 0
    while (i < args.size) {
      val shard: T = hasher.shardFor(args(i))
      if (!(shardCommands contains shard)) {
        shardCommands(shard) = collection.mutable.ArrayBuffer[ByteString]()
        indexBuffer(shard) = collection.mutable.ArrayBuffer[Int]()
      }
      var j = 0
      while (j < groupSize) {
        shardCommands(shard).append(args(i + j))
        j += 1
      }
      indexBuffer(shard).append(argIndex)
      argIndex += 1
      i += groupSize
    }

    i = 0
    val res = shardCommands.map{case (shard, args) =>
      val s = Command(cmd.utf8String, args)
      val ind = indexBuffer(shard)
      shardIndices(i) = ind.toSeq
      i += 1
      (shard, s)
    }
    (shardIndices.toMap, res.toSeq)

  }
}


class MGetScatterGather[T](orig: Command, hasher: CommandHasher[T]) extends ScatterGather[T] {

  val MGET_BYTES = ByteString(UnifiedProtocol.CMD_MGET)

  //this contains, for each scattered command, the sequence of indices in which
  //the arguments appeared in the original command, so they can be put back
  //together in the right order.  The key is the index of the command in the sequence
  var shardIndices = Map[Int, Seq[Int]]()

  def scatter: Seq[(T,Command)] = {
    val (i, s) = ScatterGather.scatterGrouped(orig.args, hasher, MGET_BYTES, 1)
    shardIndices = i
    s
  }

  def gather(replies: Seq[Reply]) = {
    val gathered: Array[Reply] = Array.fill(orig.args.size)(NilReply)
    var i = 0
    while (i < replies.size) {
      replies(i) match {
        case MBulkReply(replies) => {
          val indices = shardIndices(i)
          var j = 0
          replies.foreach{reply =>
            gathered((indices(j))) = reply
            j += 1
          }
        }
        case other => {}
      }
      i += 1
    }
    MBulkReply(gathered)
  }

}

class MSetScatterGather[T](orig: Command, hasher: CommandHasher[T]) extends ScatterGather[T]{
  val MSET_BYTES = ByteString("MSET")

  def scatter: Seq[(T,Command)] = {
    val (i, s) = ScatterGather.scatterGrouped(orig.args, hasher, MSET_BYTES, 2)
    s
  }

  def gather(replies: Seq[Reply]) = replies.collectFirst{
    case e: ErrorReply => e
  }.getOrElse(StatusReply("OK"))
}

class DelScatterGather[T](orig: Command, hasher: CommandHasher[T]) extends ScatterGather[T] {
  val DEL_BYTES = ByteString(UnifiedProtocol.CMD_DEL)

  def scatter = ScatterGather.scatterGrouped(orig.args, hasher, DEL_BYTES, 1)._2

  def gather(replies: Seq[Reply]) = replies.collectFirst{
    case e: ErrorReply => e
  }.getOrElse(IntegerReply(replies.map{
    case IntegerReply(n) => n
    case _ => 0 //hmmm
  }.sum))

}

trait BroadcastGather {
  def gather(replies: Seq[Reply]): Reply
}

object RandomKeyBroadcastGather extends BroadcastGather {
  val rand = new java.util.Random
  def gather(replies: Seq[Reply]): Reply = {
    val nonNil: Seq[BulkReply] = replies.collect{
      case b: BulkReply => b
    }
    if (nonNil.size > 0) {
      nonNil(math.abs(rand.nextInt) % nonNil.size)
    } else {
      NilReply
    }
  }
}
