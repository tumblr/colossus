package colossus

import akka.util.ByteString

class RedisHashITSpec extends BaseRedisITSpec{

  val keyPrefix = "colossusITHash"

  "Redis hash commands" should {

    val field1 = ByteString("field")
    val field2 = ByteString("field2")
    val field3 = ByteString("field3")
    val val1 = ByteString("value1")
    val val2 = ByteString("value2")
    val val3 = ByteString("value3")

    "hdel" in {
      val delKey = getKey()
      val res = for {
        _ <- client.hset(delKey, field1, val1)
        _ <- client.hset(delKey, field2, val2)
        _ <- client.hset(delKey, field3, val3)
        x <- client.hdel(delKey, field1, field2)
        y <- client.hget(delKey, field3) //should be unaffected
        z <- client.hgetOption(delKey, field1) //snould be none, since it was deleted
      } yield { (x,y,z) }

      res.futureValue must be ((2, val3, None))
    }

    "hexists" in {
      val exKey = getKey()
      val res = for {
        x <- client.hexists(exKey, field1) //false, doesn't exist yet
        y <- client.hset(exKey, field1, val1)
        z <- client.hexists(exKey, field1) //true, now exists
      } yield { (x,y,z) }

      res.futureValue must be ((false, true, true))
    }

    "hget && hset" in {
      val setKey = getKey()
      val res = for {
        x <- client.hset(setKey, field1, val1)
        y <- client.hget(setKey, field1)
      } yield { (x,y) }

      res.futureValue must be ((true, val1))
    }

    "hgetOption && hset" in {
      val setKey = getKey()
      val res = for {
        x <- client.hgetOption(setKey, field1) //none, doesn't exist yet
        y <- client.hset(setKey, field1, val1)
        z <- client.hgetOption(setKey, field1) //should now return a value
      } yield { (x,y, z) }

      res.futureValue must be ((None, true, Some(val1)))
    }

    "hgetall" in {
      val setKey = getKey()
      val res = for {
        x <- client.hgetall(setKey) //empy list returned on non existent key
        _ <- client.hset(setKey, field1, val1)
        _ <- client.hset(setKey, field2, val2)
        _ <- client.hset(setKey, field3, val3)
        y <- client.hgetall(setKey)
      } yield { (x,y) }

      val expected = Seq(field1, val1, field2, val2, field3, val3)
      res.futureValue must be ((Nil, expected))
    }

    "hincrby" in {
      val key = getKey()
      val res = for {
        x <- client.hincrby(key, field1, 10) //should create hash
        y <- client.hincrby(key, field1, 10) //should increment value
        z <- client.hget(key, field1)
      } yield { (x,y,z) }

      res.futureValue must be ((10, 20, ByteString("20")))

    }

    "hincrbyfloat" in {
      val key = getKey()
      val res = for {
        x <- client.hincrbyfloat(key, field1, 10.5) //should create hash
        y <- client.hincrbyfloat(key, field1, 10.5) //should increment value
        z <- client.hget(key, field1)
      } yield { (x,y,z) }

      res.futureValue must be ((10.5, 21.0, ByteString("21")))

    }

    "hkeys" in {
      val setKey = getKey()
      val res = for {
        x <- client.hkeys(setKey)
        _ <- client.hset(setKey, field1, val1)
        _ <- client.hset(setKey, field2, val2)
        _ <- client.hset(setKey, field3, val3)
        y <- client.hkeys(setKey)
      } yield { (x,y) }

      val expected = Seq(field1, field2, field3)
      res.futureValue must be ((Nil, expected))
    }

    "hlen" in {
      val setKey = getKey()
      val res = for {
        x <- client.hlen(setKey)
        _ <- client.hset(setKey, field1, val1)
        _ <- client.hset(setKey, field2, val2)
        _ <- client.hset(setKey, field3, val3)
        y <- client.hlen(setKey)
      } yield { (x,y) }

      res.futureValue must be ((0, 3))
    }

    "hmget" in {
      val setKey = getKey()
      val res = for {
        x <- client.hmget(setKey, field1, field2, field3)
        _ <- client.hset(setKey, field1, val1)
        _ <- client.hset(setKey, field2, val2)
        y <- client.hmget(setKey, field1, field2, field3)
      } yield { (x,y) }

      res.futureValue must be ((Seq(None, None, None), Seq(Some(val1), Some(val2), None)))
    }

    "hmset" in {
      val setKey = getKey()
      val res = for {
        _ <- client.hmset(setKey, field1, val1)
        _ <- client.hmset(setKey, field1, val2, field3, val3)
        y <- client.hmget(setKey, field1, field2, field3)
      } yield { y }

      res.futureValue must be (Seq(Some(val2), None, Some(val3)))
    }

    "hsetnx" in {
      val setnxKey = getKey()
      val res = for {
        w <- client.hsetnx(setnxKey, field1, val1) //should create if it doesn't exist
        x <- client.hget(setnxKey, field1)
        y <- client.hsetnx(setnxKey, field1, val2)  //should not do anything
        z <- client.hget(setnxKey, field1)
      } yield { (w,x,y,z) }

      res.futureValue must be ((true, val1, false, val1))
    }

    "hvals" in {
      val setKey = getKey()
      val res = for {
        x <- client.hvals(setKey)
        _ <- client.hset(setKey, field1, val1)
        _ <- client.hset(setKey, field2, val2)
        _ <- client.hset(setKey, field3, val3)
        y <- client.hvals(setKey)
      } yield { (x,y) }

      val expected = Seq(val1, val2, val3)
      res.futureValue must be ((Nil, expected))
    }

    /*"hstrlen" in {
      val key = getKey()("colIThstrlen")
      val res = for {
        x <- client.hstrlen(key, field1)
        _ <- client.hset(key, field1, val1)
        y <- client.hstrlen(key, field1)
      } yield { (x,y) }

      res.futureValue must be (0, 6)
    }*/

  }

}
