package colossus
package protocols.redis

import akka.util.ByteString

  object UnifiedProtocol {
    val RN          = ByteString("\r\n")
    val NUM_ARGS    = ByteString("*")
    val ARG_LEN     = ByteString("$")

    val BULK_REPLY    = ByteString("$")
    val ERROR_REPLY   = ByteString("-")
    val STATUS_REPLY  = ByteString("+")
    val MBULK_REPLY   = ByteString("*")
    val INTEGER_REPLY = ByteString(":")

    //KEEP THIS IN ABC ORDER!!
    val CMD_DECR             = "DECR"
    val CMD_DECRBY           = "DECRBY"
    val CMD_DEL              = "DEL"
    val CMD_EXISTS           = "EXISTS"
    val CMD_EXPIRE           = "EXPIRE"
    val CMD_FLUSHDB          = "FLUSHDB"
    val CMD_GET              = "GET"
    val CMD_GETBIT           = "GETBIT"
    val CMD_GETRANGE         = "GETRANGE"
    val CMD_GETSET           = "GETSET"
    val CMD_HDEL             = "HDEL"
    val CMD_HGET             = "HGET"
    val CMD_HGETALL          = "HGETALL"
    val CMD_HKEYS            = "HKEYS"
    val CMD_HMGET            = "HMGET"
    val CMD_HSCAN            = "HSCAN"
    val CMD_HSET             = "HSET"
    val CMD_INFO             = "INFO"
    val CMD_INCR             = "INCR"
    val CMD_INCRBY           = "INCRBY"
    val CMD_LLEN             = "LLEN"
    val CMD_LINDEX           = "LINDEX"
    val CMD_LPOP             = "LPOP"
    val CMD_LPUSH            = "LPUSH"
    val CMD_LPUSHX           = "LPUSHX"
    val CMD_LRANGE           = "LRANGE"
    val CMD_LREM             = "LREM"
    val CMD_LTRIM            = "LTRIM"
    val CMD_MGET             = "MGET"
    val CMD_MSET             = "MSET"
    val CMD_RANDOMKEY        = "RANDOMKEY"
    val CMD_RPOP             = "RPOP"
    val CMD_RPUSH            = "RPUSH"
    val CMD_RPUSHX           = "RPUSHX"
    val CMD_SCARD            = "SCARD"
    val CMD_SET              = "SET"
    val CMD_SETNX            = "SETNX"
    val CMD_SETEX            = "SETEX"
    val CMD_SISMEMBER        = "SISMEMBER"
    val CMD_SLAVEOF          = "SLAVEOF"
    val CMD_SMEMBERS         = "SMEMBERS"
    val CMD_STRLEN           = "STRLEN"
    val CMD_TTL              = "TTL"
    val CMD_TYPE             = "TYPE"
    val CMD_ZADD             = "ZADD"
    val CMD_ZCARD            = "ZCARD"
    val CMD_ZCOUNT           = "ZCOUNT"
    val CMD_ZRANGE           = "ZRANGE"
    val CMD_ZRANGEBYSCORE    = "ZRANGEBYSCORE"
    val CMD_ZRANK            = "ZRANK"
    val CMD_ZREM             = "ZREM"
    val CMD_ZREMRANGEBYRANK  = "ZREMRANGEBYRANK"
    val CMD_ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE"
    val CMD_ZREVRANGE        = "ZREVRANGE"
    val CMD_ZREVRANGEBYSCORE = "ZREVRANGEBYSCORE"
    val CMD_ZREVRANK         = "ZREVRANK"
    val CMD_ZSCORE           = "ZSCORE"


    /**
     * These are all the commands we are ok with failing over to a slave
     */
    val readCommands = CommandTrie (
      CMD_EXISTS,
      CMD_RANDOMKEY,
      CMD_TTL,
      CMD_TYPE,
      CMD_GET,
      CMD_GETBIT,
      CMD_GETRANGE,
      CMD_GETSET,
      CMD_MGET,
      CMD_STRLEN,
      CMD_ZCARD,
      CMD_ZCOUNT,
      CMD_ZRANGE,
      CMD_ZRANGEBYSCORE,
      CMD_ZRANK,
      CMD_ZREVRANGE,
      CMD_ZREVRANGEBYSCORE,
      CMD_ZREVRANK,
      CMD_ZSCORE,
      CMD_HGET,
      CMD_HGETALL,
      CMD_HKEYS,
      CMD_HMGET,
      CMD_HSCAN,
      CMD_LLEN,
      CMD_LINDEX,
      CMD_LRANGE,
      CMD_SMEMBERS,
      CMD_SISMEMBER,
      CMD_SCARD
    )

    //complete nil reply
    val NIL_REPLY   = BULK_REPLY ++ ByteString("-1") ++ RN
    //complete empty mbulk reply
    val EMPTY_MBULK_REPLY = MBULK_REPLY ++ ByteString("0") ++ RN
    //nil mbulk reply, different from empty (usually from something like popping a non-existant list)
    val NIL_MBULK_REPLY = MBULK_REPLY ++ ByteString("-1") ++ RN
  }

