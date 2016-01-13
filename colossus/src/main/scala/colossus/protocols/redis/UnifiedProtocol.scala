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

    val BEFORE = ByteString("BEFORE")
    val AFTER = ByteString("AFTER")

    //KEEP THIS IN ABC ORDER!!
    val CMD_APPEND           = "APPEND"
    val CMD_BITCOUNT         = "BITCOUNT"
    val CMD_DECR             = "DECR"
    val CMD_DECRBY           = "DECRBY"
    val CMD_DEL              = "DEL"
    val CMD_EXISTS           = "EXISTS"
    val CMD_EXPIRE           = "EXPIRE"
    val CMD_EXPIREAT         = "EXPIREAT"
    val CMD_FLUSHDB          = "FLUSHDB"
    val CMD_GET              = "GET"
    val CMD_GETBIT           = "GETBIT"
    val CMD_GETRANGE         = "GETRANGE"
    val CMD_GETSET           = "GETSET"
    val CMD_HDEL             = "HDEL"
    val CMD_HEXISTS          = "HEXISTS"
    val CMD_HGET             = "HGET"
    val CMD_HGETALL          = "HGETALL"
    val CMD_HINCRBY          = "HINCRBY"
    val CMD_HINCRBYFLOAT     = "HINCRBYFLOAT"
    val CMD_HKEYS            = "HKEYS"
    val CMD_HLEN             = "HLEN"
    val CMD_HMGET            = "HMGET"
    val CMD_HMSET            = "HMSET"
    val CMD_HSCAN            = "HSCAN"
    val CMD_HSET             = "HSET"
    val CMD_HSETNX           = "HSETNX"
    val CMD_HSTRLEN          = "HSTRLEN"
    val CMD_HVALS            = "HVALS"
    val CMD_INFO             = "INFO"
    val CMD_INCR             = "INCR"
    val CMD_INCRBY           = "INCRBY"
    val CMD_INCRBYFLOAT      = "INCRBYFLOAT"
    val CMD_KEYS             = "KEYS"
    val CMD_LINDEX           = "LINDEX"
    val CMD_LINSERT          = "LINSERT"
    val CMD_LLEN             = "LLEN"
    val CMD_LPOP             = "LPOP"
    val CMD_LPUSH            = "LPUSH"
    val CMD_LPUSHX           = "LPUSHX"
    val CMD_LRANGE           = "LRANGE"
    val CMD_LREM             = "LREM"
    val CMD_LSET             = "LSET"
    val CMD_LTRIM            = "LTRIM"
    val CMD_MGET             = "MGET"
    val CMD_MSET             = "MSET"
    val CMD_MSETNX           = "MSETNX"
    val CMD_PERSIST          = "PERSIST"
    val CMD_PEXPIRE          = "PEXPIRE"
    val CMD_PEXPIREAT        = "PEXPIREAT"
    val CMD_PSETEX           = "PSETEX"
    val CMD_PTTL             = "PTTL"
    val CMD_RANDOMKEY        = "RANDOMKEY"
    val CMD_RENAME           = "RENAME"
    val CMD_RENAMENX         = "RENAMENX"
    val CMD_RPOP             = "RPOP"
    val CMD_RPOPLPUSH        = "RPOPLPUSH"
    val CMD_RPUSH            = "RPUSH"
    val CMD_RPUSHX           = "RPUSHX"
    val CMD_SADD             = "SADD"
    val CMD_SCARD            = "SCARD"
    val CMD_SDIFF            = "SDIFF"
    val CMD_SDIFFSTORE       = "SDIFFSTORE"
    val CMD_SET              = "SET"
    val CMD_SETNX            = "SETNX"
    val CMD_SETEX            = "SETEX"
    val CMD_SINTER           = "SINTER"
    val CMD_SINTERSTORE      = "SINTERSTORE"
    val CMD_SISMEMBER        = "SISMEMBER"
    val CMD_SLAVEOF          = "SLAVEOF"
    val CMD_SMEMBERS         = "SMEMBERS"
    val CMD_SMOVE            = "SMOVE"
    val CMD_SORT             = "SORT"
    val CMD_SPOP             = "SPOP"
    val CMD_SRANDMEMBER      = "SRANDMEMBER"
    val CMD_SREM             = "SREM"
    val CMD_STRLEN           = "STRLEN"
    val CMD_SUNION           = "SUNION"
    val CMD_SUNIONSTORE      = "SUNIONSTORE"
    val CMD_TTL              = "TTL"
    val CMD_TYPE             = "TYPE"
    val CMD_ZADD             = "ZADD"
    val CMD_ZCARD            = "ZCARD"
    val CMD_ZCOUNT           = "ZCOUNT"
    val CMD_ZINCRBY          = "ZINCRBY"
    val CMD_ZINTERSTORE      = "ZINTERSTORE"
    val CMD_ZLEXCOUNT        = "ZLEXCOUNT"
    val CMD_ZRANGE           = "ZRANGE"
    val CMD_ZRANGEBYLEX      = "ZRANGEBYLEX"
    val CMD_ZRANGEBYSCORE    = "ZRANGEBYSCORE"
    val CMD_ZRANK            = "ZRANK"
    val CMD_ZREM             = "ZREM"
    val CMD_ZREMRANGEBYLEX   = "ZREMRANGEBYLEX"
    val CMD_ZREMRANGEBYRANK  = "ZREMRANGEBYRANK"
    val CMD_ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE"
    val CMD_ZREVRANGE        = "ZREVRANGE"
    val CMD_ZREVRANGEBYLEX   = "ZREVRANGEBYLEX"
    val CMD_ZREVRANGEBYSCORE = "ZREVRANGEBYSCORE"
    val CMD_ZREVRANK         = "ZREVRANK"
    val CMD_ZSCORE           = "ZSCORE"
    val CMD_ZUNIONSTORE      = "ZUNIONSTORE"

    val SET_PARAM_NX    = ByteString("NX")
    val SET_PARAM_EX    = ByteString("EX")
    val SET_PARAM_PX    = ByteString("PX")
    val SET_PARAM_XX    = ByteString("XX")

    //complete nil reply
    val NIL_REPLY   = BULK_REPLY ++ ByteString("-1") ++ RN
    //complete empty mbulk reply
    val EMPTY_MBULK_REPLY = MBULK_REPLY ++ ByteString("0") ++ RN
    //nil mbulk reply, different from empty (usually from something like popping a non-existant list)
    val NIL_MBULK_REPLY = MBULK_REPLY ++ ByteString("-1") ++ RN
  }

