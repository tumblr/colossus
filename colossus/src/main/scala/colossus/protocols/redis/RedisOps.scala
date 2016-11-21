package colossus
package protocols.redis

import akka.util.ByteString
import colossus.service._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.higherKinds

/**
 * This trait houses the Redis API.  It contains implementations for most(not all) commands.
 * In particular, this trait makes a best attempt at providing all implementations for:
 * - Key commands
 * - String commands
 * - Hash commands
 * - List commands
 * - Set commands
 * - Zset commands
 *
 * Commands not implemented yet are
 *  - scan commands
 *  - 'admin' commands, ie commands which aren't directly related to a data type.
 *
 *  Just because these commands are not implemented, doesn't mean they cannot be used.  The implementors of this trait
 *  provide a generic 'execute' command, which allows for the execution of arbitrary [[colossus.protocols.redis.Command]]objects.
 *  The calling code is responsible for handling the raw [[colossus.protocols.redis.Reply]].
 *
 * In some cases if a command can return an optional response(ie: a bulk reply or nil reply, like 'get'), 2 variants are provided.  One which
 * will return the data directly, and fail if its not there, and another which returns an Option.  This is done to provide the user with some flexibility
 * in how they query data.  The point being, if you want to query and fail on non existence, you don't have to deal w/ an intermediate Option.
 *
 * No camelcase?  Yea..camelcasing redis commands looked and felt weird. MGet? mGet? mget? mSetNx? msetNX? etc.  So, since all redis commands are uppercased(at least in the docs),
 * I went with all lower case for the API.  I just wanted something consistent looking.  The only camelcasing is for the functions which return options ie: getOption.
 *
 *  This trait tries its best to be redis version agonstic.  It doesn't know anything about the version of redis you are communicating
 *  with, so, for example, if an 'hstrlen' command is issued to a redis server that's < 3.2, you will get an Exception.
 *
 *  Some commands(mainly zset commands) have additional arguments which alter its behavior. Instead of trying to capture all the argument combinations
 *  the function provides a varargs ByteString so that the user can provide whatever trailing flags or options they want.  Each command which has this
 *  behavior is noted in its docs.
 * @tparam M
 */
trait RedisClient[M[_]] extends LiftedClient[Redis, M] {

  import UnifiedProtocol._
  import Command.{c => cmd}
  import async._

  private implicit def bs(l : Long) : ByteString = ByteString(l.toString)
  private implicit def bs(d : Double) : ByteString = ByteString(d.toString)
  private def seconds(fd : FiniteDuration) = ByteString(fd.toSeconds.toString)
  private def millis(fd : FiniteDuration) = ByteString(fd.toMillis.toString)

  /*
  in general, almost all of the redis commands boil down to processing a single type of response.  This set of functions
  captures all of them.  This trait pretty much then just maps redis commands to these functions.
   */


  protected def executeCommand[T](c : Command, key : ByteString)(goodCase : PartialFunction[Reply, M[T]]) : M[T] = {
    executeAndMap(c) {
      goodCase orElse {
        case ErrorReply(msg) => failure(RedisException(s"Received error msg when executing ${c.command} on key ${key.utf8String} : $msg"))
        case x => failure(UnexpectedRedisReplyException(s"Unexpected response $x when executing ${c.command} on ${key.utf8String}"))
      }
    }
  }

  protected def stringReplyCommand(c : Command, key : ByteString) = executeCommand(c, key){case StatusReply(x) => success(true)}
  protected def integerReplyCommand(c : Command, key : ByteString) = executeCommand(c, key){case IntegerReply(x) => success(x)}
  protected def integerReplyOptionCommand(c : Command, key : ByteString) : M[Option[Long]] = executeCommand(c, key){
    case IntegerReply(x) => success(Some(x))
    case NilReply => success(None)
  }
  protected def integerReplyBoolCommand(c : Command, key : ByteString) = executeCommand(c, key){case IntegerReply(x) => success(x == 1)}
  protected def bulkReplyCommand(c : Command, key : ByteString) = executeCommand(c, key){case BulkReply(x) => success(x)}
  protected def bulkReplyAsDoubleCommand(c : Command, key : ByteString) = executeCommand(c, key){case BulkReply(x) => success(x.utf8String.toDouble)}
  protected def bulkReplyAsDoubleOptionCommand(c : Command, key : ByteString) : M[Option[Double]] = executeCommand(c, key){
    case BulkReply(x) => success(Some(new String(x.toArray).toDouble))
    case NilReply => success(None)
  }

  protected def bulkReplyOptionCommand(c : Command, key : ByteString) : M[Option[ByteString]] = executeCommand(c, key){
    case BulkReply(x) => success(Some(x))
    case NilReply => success(None)
  }
  protected def mBulkReplyCommand(c : Command, key : ByteString) : M[Seq[ByteString]] = executeCommand(c, key){
    case MBulkReply(x) => {
      val b = x.collect{
        case BulkReply(y) => y
      }
      if(b.length == x.length){
        success(b)
      }else{
        failure(UnexpectedRedisReplyException(s"Unexpected Reply type found in MBulkReply, expected all BulkReplies when executing ${c.command} on ${key.utf8String}"))
      }
    }
    case EmptyMBulkReply => success(Nil)
  }

  protected def mBulkReplyOptionCommand(c : Command, key : ByteString) : M[Seq[Option[ByteString]]] = executeCommand(c, key) {
    case MBulkReply(x) => {
      val b = x.collect{
        case BulkReply(y) => Some(y)
        case NilReply => None
      }
      if(b.length == x.length){
        success(b)
      }else{
        failure(UnexpectedRedisReplyException(s"Unexpected Reply type found in MBulkReply, expected all BulkReply or NilReply when executing ${c.command} on ${key.utf8String}"))
      }
    }
  }

  private def multiKey(keys : Seq[ByteString]) = {
    if(keys.isEmpty){
      ByteString("no keys")
    }else if(keys.size == 1){
      keys.head
    }else{
      keys.head ++ ByteString(s" and ${keys.size} other keys")
    }
  }

  //begin public API

  def append(key : ByteString, value : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_APPEND, key, value), key)

  def decr(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_DECR, key), key)

  def decrBy(key : ByteString, amount : Long) : M[Long] = integerReplyCommand(cmd(CMD_DECRBY, key, amount), key)

  def del(keys : ByteString*) : M[Long] = integerReplyCommand(cmd(CMD_DEL, keys : _*), multiKey(keys))

  def exists(key : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_EXISTS, key), key)

  def expire(key : ByteString, ttl : FiniteDuration) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_EXPIRE, key, seconds(ttl)), key)

  def expireat(key : ByteString, unixTimeInSeconds : Long) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_EXPIREAT, key, unixTimeInSeconds), key)

  def get(key : ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_GET, key), key)

  def getOption(key : ByteString) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_GET, key), key)

  def getrange(key : ByteString, from : Long, to : Long) : M[ByteString] = bulkReplyCommand(cmd(CMD_GETRANGE, key, from, to), key)

  def getset(key : ByteString, value : ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_GETSET, key, value), key)

  def getsetOption(key : ByteString, value : ByteString) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_GETSET, key, value), key)

  def hdel(key : ByteString, fields : ByteString*) = integerReplyCommand(cmd(CMD_HDEL, key +: fields : _*), key)

  def hexists(key : ByteString, field : ByteString) = integerReplyBoolCommand(cmd(CMD_HEXISTS, key, field), key)

  def hget(key : ByteString, field : ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_HGET, key, field), key)

  def hgetOption(key : ByteString, field : ByteString) = bulkReplyOptionCommand(cmd(CMD_HGET, key, field), key)

  //todo: change return type to map
  def hgetall(key : ByteString) : M[Seq[ByteString]] = mBulkReplyCommand(cmd(CMD_HGETALL, key), key)

  def hincrby(key : ByteString, field : ByteString, amount : Long) : M[Long] = integerReplyCommand(cmd(CMD_HINCRBY, key, field, amount), key)

  def hincrbyfloat(key : ByteString, field : ByteString, amount : Double) : M[Double] =
    bulkReplyAsDoubleCommand(cmd(CMD_HINCRBYFLOAT, key, field, amount), key)

  def hkeys(key : ByteString) : M[Seq[ByteString]] = mBulkReplyCommand(cmd(CMD_HKEYS, key), key)

  def hlen(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_HLEN, key), key)

  def hmget(key : ByteString, fields  : ByteString*) : M[Seq[Option[ByteString]]] = mBulkReplyOptionCommand(cmd(CMD_HMGET, key +: fields : _*), key)

  def hmset(key : ByteString, fields : ByteString*) : M[Boolean] = stringReplyCommand(cmd(CMD_HMSET, key +: fields :_ *), key)

  def hset(key : ByteString, field : ByteString, value : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_HSET, key, field, value), key)

  def hsetnx(key : ByteString, field : ByteString, value : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_HSETNX, key, field, value), key)

  def hstrlen(key : ByteString, field : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_HSTRLEN, key, field), key)

  def hvals(key : ByteString) : M[Seq[ByteString]] = mBulkReplyCommand(cmd(CMD_HVALS, key), key)

  def incr(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_INCR, key), key)

  def incrby(key : ByteString, amount : Long) : M[Long] = integerReplyCommand(cmd(CMD_INCRBY, key, amount), key)

  def incrbyfloat(key : ByteString, amount : Double) : M[Double] = bulkReplyAsDoubleCommand(cmd(CMD_INCRBYFLOAT, key, amount), key)

  def keys(key : ByteString) : M[Seq[ByteString]] = mBulkReplyCommand(cmd(CMD_KEYS, key), key)

  def lindex(key : ByteString, index : Long) : M[ByteString] = bulkReplyCommand(cmd(CMD_LINDEX, key, index), key)

  def lindexOption(key : ByteString, index : Long) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_LINDEX, key, index), key)

  def linsertBefore(key : ByteString, pivotValue : ByteString, value : ByteString) : M[Long] =
    integerReplyCommand(cmd(CMD_LINSERT, key, BEFORE, pivotValue, value), key)

  def linsertAfter(key : ByteString, pivotValue : ByteString, value : ByteString) : M[Long] =
    integerReplyCommand(cmd(CMD_LINSERT, key, AFTER, pivotValue, value), key)

  def llen(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_LLEN, key), key)

  def lpop(key : ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_LPOP, key), key)

  def lpopOption(key : ByteString) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_LPOP, key), key)

  def lpush(key : ByteString, values : ByteString*) : M[Long] = integerReplyCommand(cmd(CMD_LPUSH, key +: values :_*), key)

  def lpushx(key : ByteString, value : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_LPUSHX, key, value), key)

  def lrange(key : ByteString, start : Long, end : Long) : M[Seq[ByteString]] = mBulkReplyCommand(cmd(CMD_LRANGE, key, start, end), key)

  def lrem(key : ByteString, count : Long, value : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_LREM, key, count, value), key)

  def lset(key : ByteString, index : Long, value : ByteString) : M[Boolean] = stringReplyCommand(cmd(CMD_LSET, key, index, value), key)

  def ltrim(key : ByteString, start : Long, end : Long) : M[Boolean] = stringReplyCommand(cmd(CMD_LTRIM, key, start, end), key)

  def mget(keys : ByteString*) : M[Seq[Option[ByteString]]] = mBulkReplyOptionCommand(cmd(CMD_MGET, keys : _*), multiKey(keys))

  def mset(keysAndValues : ByteString*) : M[Boolean] = stringReplyCommand(cmd(CMD_MSET, keysAndValues : _*), multiKey(keysAndValues))

  def msetnx(keysAndValues : ByteString*) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_MSETNX, keysAndValues : _*), multiKey(keysAndValues))

  def persist(key : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_PERSIST, key), key)

  def pexpire(key : ByteString, ttl : FiniteDuration) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_PEXPIRE, key, millis(ttl)), key)

  def pexpireat(key : ByteString, unixTimeInMillis : Long) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_PEXPIREAT, key, unixTimeInMillis), key)

  def psetex(key : ByteString, value : ByteString, ttl : FiniteDuration) : M[Boolean] = stringReplyCommand(cmd(CMD_PSETEX, key, millis(ttl), value), key)

  def pttl(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_PTTL, key), key)

  def randomkey() : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_RANDOMKEY), ByteString(""))

  def rename(source : ByteString, destination : ByteString) : M[Boolean] = stringReplyCommand(cmd(CMD_RENAME, source, destination), source)

  def renamenx(source : ByteString, destination : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_RENAMENX, source, destination), source)

  def rpop(key : ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_RPOP, key), key)

  def rpopOption(key : ByteString) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_RPOP, key), key)

  def rpoplpush(key : ByteString, destination : ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_RPOPLPUSH, key, destination), key)

  def rpoplpushOption(key : ByteString, destination : ByteString) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_RPOPLPUSH, key, destination), key)

  def rpush(key : ByteString, values : ByteString*) : M[Long] = integerReplyCommand(cmd(CMD_RPUSH, key +: values :_*), key)

  def rpushx(key : ByteString, value : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_RPUSHX, key, value), key)

  def sadd(key : ByteString, values : ByteString*) : M[Long] = integerReplyCommand(cmd(CMD_SADD, key +: values : _*), key)

  def scard(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_SCARD, key), key)

  def sdiff(key : ByteString, keys : ByteString*) : M[Set[ByteString]] =
    map(mBulkReplyCommand(cmd(CMD_SDIFF, key +: keys : _*), key))(_.toSet)

  def sdiffstore(destination : ByteString, key : ByteString, keys : ByteString*) : M[Long] =
    integerReplyCommand(cmd(CMD_SDIFFSTORE, destination +: key +: keys : _*), key)

  def set(key : ByteString, value : ByteString, notExists: Boolean = false, exists: Boolean = false, ttl: Duration = Duration.Inf) : M[Boolean] = {
    var args = Seq(key, value)
    if (notExists) {
      args = args :+ SET_PARAM_NX
    } else if (exists) {
      args = args :+ SET_PARAM_XX
    }
    if (ttl.isFinite) {
      args = args :+ SET_PARAM_PX
      args = args :+ ByteString(ttl.toMillis.toString)
    }
    executeCommand(Command(CMD_SET, args), key){
      case StatusReply(_) => success(true)
      case NilReply       => success(false)
    }
  }

  def setnx(key : ByteString, value : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_SETNX, key, value), key)

  def setex(key : ByteString, value : ByteString, ttl : FiniteDuration) : M[Boolean] =
    stringReplyCommand(cmd(CMD_SETEX, key, seconds(ttl), value), key)

  def sinter(key : ByteString, keys : ByteString*) : M[Set[ByteString]] = map(mBulkReplyCommand(cmd(CMD_SINTER, key +: keys : _*), key))(_.toSet)

  def sinterstore(destination : ByteString, key : ByteString, keys : ByteString*) : M[Long] =
    integerReplyCommand(cmd(CMD_SINTERSTORE, destination +: key +: keys : _*), key)

  def sismember(key : ByteString, value : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_SISMEMBER, key, value), key)

  def smembers(key : ByteString) : M[Set[ByteString]] = map(mBulkReplyCommand(cmd(CMD_SMEMBERS, key), key))(_.toSet)

  def smove(source : ByteString, destination : ByteString, value : ByteString) : M[Boolean] = integerReplyBoolCommand(cmd(CMD_SMOVE, source, destination, value), source)

  def spop(key: ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_SPOP, key), key)

  def spopOption(key: ByteString) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_SPOP, key), key)

  def srandmember(key : ByteString) : M[ByteString] = bulkReplyCommand(cmd(CMD_SRANDMEMBER, key), key)

  def srandmemberOption(key : ByteString) : M[Option[ByteString]] = bulkReplyOptionCommand(cmd(CMD_SRANDMEMBER, key), key)

  def srandmember(key : ByteString, count : Long) : M[Seq[ByteString]] = mBulkReplyCommand(cmd(CMD_SRANDMEMBER, key, ByteString(count.toString)), key)

  def srem(key : ByteString, values : ByteString*) : M[Long] = integerReplyCommand(cmd(CMD_SREM, key +: values :_*), key)

  def strlen(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_STRLEN, key), key)

  def sunion(key : ByteString, keys : ByteString*) : M[Set[ByteString]] = map(mBulkReplyCommand(cmd(CMD_SUNION, key +: keys : _*), key))(_.toSet)

  def sunionstore(destination : ByteString, key : ByteString, keys : ByteString*) : M[Long] =
    integerReplyCommand(cmd(CMD_SUNIONSTORE, destination +: key +: keys : _*), key)

  def ttl(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_TTL, key), key)

  /**
   * This is one of the aforementioned special commands. Zadd takes a host of arguments versions >= 3.0.2, as noted in
   * http://redis.io/commands/zadd
   * Since this API *tries* to be redis version agnostic, it doesn't have them formally in its function definition, but they
   * can be passed in at the beginning of the valuesAndArguments varargs parameter:
   * {{{
   * client.zadd("key", "XX", "CH" "1", "a"....
   * }}}
   *
   * Also, since these flags can change the return type, can be a Long in some cases and a Double in others, a Double is returned to capture both cases.
   * @param key
   * @param valuesAndArguments
   * @return
   */
  def zadd(key : ByteString, valuesAndArguments : ByteString*) : M[Double] = executeCommand(cmd(CMD_ZADD, key +: valuesAndArguments :_*), key){
    case IntegerReply(x) => success(x)
    case BulkReply(x) => success(x.utf8String.toDouble)
  }

  def zcard(key : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_ZCARD, key), key)

  def zcount(key : ByteString, from : Option[Double] = None, to : Option[Double]) : M[Long] = {
    val fromB = from.fold(ByteString("-inf")){x => ByteString(x.toString)}
    val toB = to.fold(ByteString("+inf")){x => ByteString(x.toString)}
    integerReplyCommand(cmd(CMD_ZCOUNT, key, fromB, toB), key)
  }

  def zincrby(key : ByteString, amount : Double, value : ByteString) : M[Double] = bulkReplyAsDoubleCommand(cmd(CMD_ZINCRBY, key, bs(amount), value), key)


  /**
   * Another command which takes optional arguments.  Tack on additional arguments into the keysAndArguments varargs.
   * http://redis.io/commands/zinterstore
   * {{{
   *   client.zinterstore(key, 2, keyA, keyB, "WEIGHTS", "1", "2", "AGGREGATE" "SUM")
   * }}}
   * @param key
   * @param numKeys
   * @param keysAndArguments
   * @return
   */
  def zinterstore(key : ByteString, numKeys : Long, keysAndArguments : ByteString*) : M[Long] =
    integerReplyCommand(cmd(CMD_ZINTERSTORE, key +: bs(numKeys) +: keysAndArguments :_*), key)

  def zlexcount(key : ByteString, min : ByteString = ByteString("-"), max : ByteString = ByteString("+")) : M[Long] =
    integerReplyCommand(cmd(CMD_ZLEXCOUNT, key, min, max), key)

  //pass in 'withscores' to get scores.  Future versions will have a function for this.
  /**
   * This command can take a 'withscores' additional argument at the end.  It changes what is included in the the results.
   *
   * http://redis.io/commands/zrange
   *
   * For now, this function is a bit raw, and a future change will have 2 versions of this function.
   * @param key
   * @param start
   * @param stop
   * @param arguments
   * @return
   */
  def zrange(key : ByteString, start : Long, stop : Long, arguments : ByteString*) : M[Seq[ByteString]] =
    mBulkReplyCommand(cmd(CMD_ZRANGE, key +: bs(start) +:  bs(stop) +: arguments :_*), key)

  /*def zrangeWithScores(key : ByteString, from : Double, to : Double) : M[Seq[(ByteString, Double)]] = {
    map(mBulkReplyCommand(cmd(CMD_ZRANGE, bs(from), bs(to), ByteString("withscores")), key)){ x =>
      x.foldLeft(Vector[(ByteString, Double)]()){case (acc, i)

      }
    }
  }*/

  /**
   * Additional arguments for this function are the limit/offset/count options.
   *
   * http://redis.io/commands/zrangebylex
   *
   * @param key
   * @param min
   * @param max
   * @param arguments
   * @return
   */
  def zrangebylex(key : ByteString, min : ByteString, max : ByteString, arguments : ByteString*) : M[Seq[ByteString]] =
    mBulkReplyCommand(cmd(CMD_ZRANGEBYLEX, key +: min +: max +: arguments : _*), key)


  /**
   * Additional arguments for this function are the limit/offset/count options.
   *
   * http://redis.io/commands/zrangebyscore
   * 
   * @param key
   * @param min
   * @param max
   * @param arguments
   * @return
   */
  def zrangebyscore(key : ByteString, min : ByteString, max : ByteString, arguments : ByteString*) : M[Seq[ByteString]] =
    mBulkReplyCommand(cmd(CMD_ZRANGEBYSCORE, key +: min +: max +: arguments : _*), key)

  def zrank(key : ByteString, value : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_ZRANK, key, value), key)

  def zrankOption(key : ByteString, value : ByteString) : M[Option[Long]] = integerReplyOptionCommand(cmd(CMD_ZRANK, key, value), key)

  def zrem(key : ByteString, values : ByteString*) : M[Long] = integerReplyCommand(cmd(CMD_ZREM, key +: values :_*), key)

  def zremrangebylex(key : ByteString, min : ByteString, max : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_ZREMRANGEBYLEX, key, min, max), key)

  def zremrangebyrank(key : ByteString, start : Long, stop : Long) : M[Long] = integerReplyCommand(cmd(CMD_ZREMRANGEBYRANK, key, bs(start), bs(stop)), key)

  def zremrangebyscore(key : ByteString, min : ByteString, max : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_ZREMRANGEBYSCORE, key, min, max), key)

  /**
   * This command can take a 'withscores' additional argument at the end.  It changes what is included in the the results.
   *
   * http://redis.io/commands/zrevrange
   *
   * For now, this function is a bit raw, and a future change will have 2 versions of this function.
   * @param key
   * @param start
   * @param stop
   * @param arguments
   * @return
   */
  def zrevrange(key : ByteString, start : Long, stop : Long, arguments : ByteString*) : M[Seq[ByteString]] =
    mBulkReplyCommand(cmd(CMD_ZREVRANGE, key +: bs(start) +:  bs(stop) +: arguments :_*), key)

  /**
   * Additional arguments for this function are the limit/offset/count options.
   *
   * http://redis.io/commands/zrevrangebylex
   *
   * @param key
   * @param max
   * @param min
   * @param arguments
   * @return
   */
  def zrevrangebylex(key : ByteString, max : ByteString, min : ByteString, arguments : ByteString*) : M[Seq[ByteString]] =
    mBulkReplyCommand(cmd(CMD_ZREVRANGEBYLEX, key +: max +: min +: arguments : _*), key)


  /**
   *
   * Additional arguments for this function are the limit/offset/count options.
   *
   * http://redis.io/commands/zrevrangebyscore
   *
   * @param key
   * @param max
   * @param min
   * @param arguments
   * @return
   */
  def zrevrangebyscore(key : ByteString, max : ByteString, min : ByteString, arguments : ByteString*) : M[Seq[ByteString]] =
    mBulkReplyCommand(cmd(CMD_ZREVRANGEBYSCORE, key +: max +: min +: arguments : _*), key)

  def zrevrank(key : ByteString, value : ByteString) : M[Long] = integerReplyCommand(cmd(CMD_ZREVRANK, key, value), key)

  def zrevrankOption(key : ByteString, value : ByteString) : M[Option[Long]] = integerReplyOptionCommand(cmd(CMD_ZREVRANK, key, value), key)

  def zscore(key : ByteString, value : ByteString) : M[Double] = bulkReplyAsDoubleCommand(cmd(CMD_ZSCORE, key, value), key)

  def zscoreOption(key : ByteString, value : ByteString) : M[Option[Double]] = bulkReplyAsDoubleOptionCommand(cmd(CMD_ZSCORE, key, value), key)

  /**
   * Another command which takes optional arguments.  Tack on additional arguments into the keysAndArguments varargs.
   * http://redis.io/commands/zunionstore
   * {{{
   *   client.zunionstore(key, 2, keyA, keyB, "WEIGHTS", "1", "2", "AGGREGATE" "SUM")
   * }}}
   * @param key
   * @param numKeys
   * @param keysAndArguments
   * @return
   */
  def zunionstore(key : ByteString, numKeys : Long, keysAndArguments : ByteString*) : M[Long] =
    integerReplyCommand(cmd(CMD_ZUNIONSTORE, key +: bs(numKeys) +: keysAndArguments :_*), key)

}

object RedisClient {

  implicit object RedisClientLifter extends ClientLifter[Redis, RedisClient] {
    
    def lift[M[_]](client: Sender[Redis,M], clientConfig: Option[ClientConfig])(implicit async: Async[M]) = {
      new BasicLiftedClient(client, clientConfig) with RedisClient[M]
    }
  }

}


case class RedisException(message : String) extends Exception

case class UnexpectedRedisReplyException(message : String) extends Exception
