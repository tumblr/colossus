package colossus

import akka.util.ByteString

import scala.concurrent.duration._
import colossus.protocols.redis.{IntegerReply, Command}

class RedisITSpec extends BaseRedisITSpec{

  val keyPrefix = "colossusKeyIT"

  "Redis key and string commands" should {

    val value = ByteString("value")

    "append" in {
      val appKey = getKey()

      val res = for {
        x <- client.append(appKey, value) //should create if key doesn't exist
        y <- client.append(appKey, value) //should append
      } yield { (x,y) }

      res.futureValue must be ((5,10))
    }

    "decr" in {
      val key = getKey()

      val res = for {
        x <- client.decr(key) //should create if key doesn't exist
        y <- client.decr(key)
        z <- client.get(key)
      } yield { (x,y,z) }

      res.futureValue must be ((-1, -2, ByteString("-2")))
    }

    "decrby" in {
      val key = getKey()

      val res = for {
        x <- client.decrBy(key, 3) //should create if key doesn't exist
        y <- client.decrBy(key, 3)
        z <- client.get(key)
      } yield { (x,y,z) }

      res.futureValue must be ((-3, -6, ByteString("-6")))

    }

    "del" in {
      val delKey = getKey()

      val res = for {
        w <- client.del(delKey)
        x <- client.set(delKey, value)
        y <- client.del(delKey)
      } yield { (w,x,y) }

      res.futureValue must be ((0, true, 1))
    }

    "exists" in {
      val exKey = getKey()
      val res = for {
        x <- client.exists(exKey)
        y <- client.set(exKey, value)
        z <- client.exists(exKey)
      } yield { (x,y,z) }

      res.futureValue must be ((false, true, true))
    }

    "expire" in {
      val key = getKey()
      val res = for{
        x <- client.expire(key, 1.second) //doesn't exist, should get false
        y <- client.set(key, value) //set key
        z <- client.expire(key, 10.seconds) //create expiration
        ttl <- client.ttl(key) //get ttl
      } yield { (x,y,z,ttl >= 0) }

      //not checking for the actual TTL, that value will be hard to pin down, just its existence is good
      res.futureValue must be((false, true, true, true))
    }

    "expireat" in {
      val key = getKey()
      val tomorrow = (System.currentTimeMillis() / 1000) + 86400
      val res = for{
        x <- client.expireat(key, tomorrow)
        y <- client.set(key, value)
        z <- client.expireat(key, tomorrow)
        ttl <- client.ttl(key)
      } yield { (x,y,z, ttl >= 0) }

      //not checking for the actual TTL, that value will be hard to pin down, just its existence is good
      res.futureValue must be((false, true, true, true))
    }

    "get && set" in {
      val setKey = getKey()
      val res = for {
        x <- client.set(setKey, value)
        y <- client.get(setKey)
      } yield { (x,y) }

      res.futureValue must be ((true, value))
    }

    "getOption && set" in {
      val setKey = getKey()
      val res = for {
        x <- client.getOption(setKey)
        y <- client.set(setKey, value)
        z <- client.getOption(setKey)
      } yield { (x,y, z) }

      res.futureValue must be ((None, true, Some(value)))
    }

    "getrange" in {
      val setKey = getKey()
      val res = for {
        w <- client.getrange(setKey, 0, 1)
        x <- client.set(setKey, value)
        y <- client.getrange(setKey, 0, 1)
      } yield { (w,x,y) }

      res.futureValue must be ((ByteString(""), true, ByteString("va")))
    }

    "getset" in {
      val key = getKey()
      val res = for {
        x <- client.set(key, value)
        y <- client.getset(key, ByteString("value2"))
        z <- client.get(key)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((true, value, ByteString("value2")))
    }

    "getsetOption" in {
      val key = getKey()
      val res = for {
        x <- client.getsetOption(key, value)
        y <- client.getsetOption(key, ByteString("value2"))
        z <- client.get(key)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((None, Some(value), ByteString("value2")))
    }

    "incr" in {
      val key = getKey()

      val res = for {
        x <- client.incr(key)
        y <- client.incr(key)
        z <- client.get(key)
      } yield { (x,y,z) }

      res.futureValue must be ((1,2, ByteString("2")))
    }

    "incrby" in {
      val key = getKey()
      val res = for {
        x <- client.incrby(key, 10)
        y <- client.incrby(key, 10)
        z <- client.get(key)
      } yield { (x,y,z) }

      res.futureValue must be ((10, 20, ByteString("20")))

    }

    "incrbyfloat" in {
      val key = getKey()
      val res = for {
        x <- client.incrbyfloat(key, 10.25)
        y <- client.incrbyfloat(key, 10.25)
        z <- client.get(key)
      } yield { (x,y,z) }

      res.futureValue must be ((10.25, 20.5, ByteString("20.5")))
    }

    "keys" in {
      val key1 = getKey("colossusKeysKeysIT")
      val key2 = getKey("colossusKeysKeysIT")
      val res = for {
        _ <- client.set(key1, value)
        _ <- client.set(key2, value)
        x <- client.keys(ByteString("colossusKeysKeysIT*"))
      } yield {
          x
        }
      res.futureValue.toSet must be (Set(key1, key2))
    }

    "mget" in {
      val key1 = getKey()
      val key2 = getKey()
      val key3 = getKey() //intentionally left blank
      val res = for {
        _ <- client.set(key1, value)
        _ <- client.set(key2, value)
        x <- client.mget(key1, key2, key3)
      } yield {
        x
      }
      res.futureValue must be (Seq(Some(value), Some(value), None))
    }

    "mset" in {
      val key1 = getKey()
      val key2 = getKey()
      val res = for {
        x <- client.mset(key1, value, key2, value)
        y <- client.mget(key1, key2)
      } yield {
          (x,y)
        }

      res.futureValue must be ((true, Seq(Some(value), Some(value))))
    }

    "msetnx" in {
      val key1 = getKey()
      val key2 = getKey()
      val key3 = getKey()
      val res = for {
        x <- client.msetnx(key1, value, key2, value) //should work, both keys don't exist
        y <- client.msetnx(key1, value, key3, value) //should not work, one key is already set
        z <- client.mget(key1, key2, key3)
      } yield {
          (x,y,z)
        }

      res.futureValue must be((true, false, Seq(Some(value), Some(value), None)))
    }

    "persist" in {

      val key1 = getKey()
      val res = for {
        w <- client.persist(key1) //non existent, should be false
        x <- client.setex(key1, value, 10.minutes) //set value
        y <- client.persist(key1) //should remove ttl
        z <- client.ttl(key1)  //should not be set
      } yield {
          (w,x,y,z >= 0)
        }

      res.futureValue must be ((false, true, true, false))
    }

    "pexpire" in {
      val key = getKey()
      val res = for{
        x <- client.pexpire(key, 1.second) //doesn't exist, should get false
        y <- client.set(key, value) //set key
        z <- client.pexpire(key, 10.seconds) //create expiration
        ttl <- client.ttl(key) //get ttl
      } yield { (x,y,z,ttl >= 0) }

      //not checking for the actual TTL, that value will be hard to pin down, just its existence is good
      res.futureValue must be((false, true, true, true))
    }

    "pexpireat" in {
      val key = getKey()
      val tomorrow = (System.currentTimeMillis()) + 86400
      val res = for{
        x <- client.pexpireat(key, tomorrow)
        y <- client.set(key, value)
        z <- client.pexpireat(key, tomorrow)
        ttl <- client.ttl(key)
      } yield { (x,y,z, ttl >= 0) }

      //not checking for the actual TTL, that value will be hard to pin down, just its existence is good
      res.futureValue must be((false, true, true, true))
    }

    "psetex" in {
      val setexKey = getKey()
      val res = for {
        w <- client.ttl(setexKey)
        x <- client.psetex(setexKey, value, 10.seconds)
        y <- client.get(setexKey)
        z <- client.ttl(setexKey) //can't test for exact value
      } yield { (w >= 0,x,y,z >= 0) }

      res.futureValue must be ((false, true, value, true))
    }

    "pttl" in {
      val key = getKey()
      val res = for {
        w <- client.pttl(key)
        x <- client.setex(key, value, 10.seconds)
        y <- client.get(key)
        z <- client.pttl(key) //can't test for exact value
      } yield { (w >= 0,x,y,z >= 0) }

      res.futureValue must be ((false, true, value, true))
    }

    "randomkey" in {
      val key = getKey()
      val key2 = getKey()
      val res = for {
        _ <- client.mset(key, value, key2, value)
        x <- client.randomkey()
      } yield {
        x.isDefined
      }
      res.futureValue must be (true)
    }

    "rename" in {
      val key = getKey()
      val key2 = getKey()
      val res = for {
        _ <- client.set(key, value)
        x <- client.rename(key, key2)
        y <- client.mget(key, key2)
      } yield {
          (x, y)
        }

      res.futureValue must be((true, Seq(None, Some(value))))
    }

    "renamenx" in {
      val key = getKey()
      val key2 = getKey()
      val key3 = getKey()
      val res = for {
        _ <- client.mset(key, value, key2, value)
        x <- client.renamenx(key, key2) //should fail
        y <- client.renamenx(key, key3)
        z <- client.mget(key, key2, key3)
      } yield {
          (x, y, z)
        }

      res.futureValue must be((false, true, Seq(None, Some(value), Some(value))))
    }

    "set (basic)" in {
      val key = getKey()
      client.set(key, value).futureValue must be(true)
    }

    "set (with NX)" in {
      val key = getKey()
      val key2 = getKey()
      val res = for {
        x <- client.set(key, value)
        y <- client.set(key, value, notExists = true)
        z <- client.set(key2, value, notExists = true)
      } yield (x, y, z)
      res.futureValue must be ((true, false, true))
    }

    "set (with EX)" in {
      val key = getKey()
      val key2 = getKey()
      val res = for {
        x <- client.set(key, value)
        y <- client.set(key, value, exists = true)
        z <- client.set(key2, value, exists = true)
      } yield (x, y, z)
      res.futureValue must be ((true, true, false))
    }

    "set (with ttl)" in {
      val key = getKey()
      val res = for {
        x <- client.set(key, value, ttl = 10.seconds)
        y <- client.ttl(key)
      } yield (x, y >= 0 && y <= 10)
      res.futureValue must be ((true, true))
    }

    "setnx" in {
      val setnxKey = getKey()

      val res = for {
        x <- client.setnx(setnxKey, value)
        y <- client.get(setnxKey)
        z <- client.setnx(setnxKey, value)
      } yield { (x,y,z) }

      res.futureValue must be ((true, value, false))
    }

    "setex && ttl" in {
      val setexKey = getKey()
      val res = for {
        w <- client.ttl(setexKey)
        x <- client.setex(setexKey, value, 10.seconds)
        y <- client.get(setexKey)
        z <- client.ttl(setexKey) //can't test for exact value
      } yield { (w >= 0,x,y,z >= 0) }

      res.futureValue must be ((false, true, value, true))
    }

    "strlen" in {
      val strlenKey = getKey()
      val res = for {
        w <- client.strlen(strlenKey)
        x <- client.set(strlenKey, value)
        y <- client.strlen(strlenKey)
      } yield { (w,x,y) }

      res.futureValue must be (0, true, 5)
    }

    "generic command" in {
      client.send(Command.c("DBSIZE")).map{
        case IntegerReply(x) => //sweet
        case other => throw new Exception(s"bad response! $other")
      }
    }
  }
}
