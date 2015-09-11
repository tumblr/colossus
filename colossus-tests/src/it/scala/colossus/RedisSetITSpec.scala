package colossus

import akka.util.ByteString

class RedisSetITSpec extends BaseRedisITSpec {

  val keyPrefix = "colossusITSet"

  val val1 = ByteString("value1")
  val val2 = ByteString("value2")
  val val3 = ByteString("value3")
  val val4 = ByteString("value4")

  "Redis set commands" should {

    "sadd" in {
      val key = getKey()
      val res = for {
        x <- client.sadd(key, val1, val2, val3)
        y <- client.smembers(key)
      } yield {
        (x, y)
      }

      res.futureValue must be ((3, Set(val1, val2, val3)))
    }

    "scard" in {

      val key = getKey()
      val res = for {
        x <- client.sadd(key, val1, val2, val3)
        y <- client.scard(key)
      } yield {
          (x, y)
        }

      res.futureValue must be ((3, 3))
    }

    "sdiff" in {
      val key1 = getKey()
      val key2 = getKey()
      val res = for {
        _ <- client.sadd(key1, val1)
        _ <- client.sadd(key2, val2)
        x <- client.sdiff(key1, key2)
      } yield {
          x
        }

      res.futureValue must be (Set(val1))
    }

    "sdiffstore" in {
      val key1 = getKey()
      val key2 = getKey()
      val key3 = getKey()
      val res = for {
        _ <- client.sadd(key1, val1)
        _ <- client.sadd(key2, val2)
        x <- client.sdiffstore(key3, key1, key2)
        y <- client.smembers(key3)
      } yield {
          (x, y)
        }

      res.futureValue must be ((1,Set(val1)))
    }

    "sinter" in {
      val key1 = getKey()
      val key2 = getKey()
      val res = for {
        _ <- client.sadd(key1, val1)
        _ <- client.sadd(key2, val1, val2)
        x <- client.sinter(key1, key2)
      } yield {
          x
        }

      res.futureValue must be (Set(val1))
    }

    "sinterstore" in {
      val key1 = getKey()
      val key2 = getKey()
      val key3 = getKey()
      val res = for {
        _ <- client.sadd(key1, val1)
        _ <- client.sadd(key2, val1, val2)
        x <- client.sinterstore(key3, key1, key2)
        y <- client.smembers(key3)
      } yield {
          (x, y)
        }

      res.futureValue must be ((1,Set(val1)))
    }

    "sismember" in {
      val key = getKey()
      val res = for {
        w <- client.sismember(key, val1)
        _ <- client.sadd(key, val1)
        x <- client.sismember(key, val1)
        y <- client.sismember(key, val2)
      } yield {
          (w, x, y)
        }

      res.futureValue must be ((false, true, false))
    }

    "smembers" in {
      val key = getKey()
      val res = for {
        x <- client.smembers(key)
        _ <- client.sadd(key, val1, val2, val3)
        y <- client.smembers(key)
      } yield {
          (x, y)
        }

      res.futureValue must be ((Set(), Set(val1, val2, val3)))

    }

    "smove" in {

      val key = getKey()
      val destKey = getKey()
      val res = for {
        _ <- client.sadd(key, val1, val2 )
        _ <- client.sadd(destKey, val2)
        w <- client.smove(key, destKey, val1)
        x <- client.scard(key)
        y <- client.scard(destKey)
      }yield {
        (w,x,y)
      }
      res.futureValue must be ((true, 1, 2))
    }

    "spop" in {
      val key = getKey()
      val res = for {
        _ <- client.sadd(key, val1, val2)
        x <- client.spop(key)
        y <- client.scard(key)
      }yield{
          (Set(val1, val2).contains(x) ,y)
        }

      res.futureValue mustBe((true, 1))
    }

    "spopOption" in {
      val key = getKey()
      val res = for {
        x <- client.spopOption(key)
        _ <- client.sadd(key, val1, val2)
        y <- client.spopOption(key)
        z <- client.scard(key)
      }yield{
          (x,  y.map(Set(val1, val2).contains(_)) ,z)
        }

      res.futureValue mustBe((None, Some(true), 1))
    }

    "srandmember" in {
      val key = getKey()
      val res = for {
        _ <- client.sadd(key, val1, val2)
        x <- client.srandmember(key)
        y <- client.scard(key)
      }yield{
          (Set(val1, val2).contains(x) ,y)
        }

      res.futureValue mustBe((true, 2))
    }

    "srandmemberOption" in {
      val key = getKey()
      val res = for {
        x <- client.srandmemberOption(key)
        _ <- client.sadd(key, val1, val2)
        y <- client.srandmemberOption(key)
        z <- client.scard(key)
      }yield{
          (x,  y.map(Set(val1, val2).contains(_)) ,z)
        }

      res.futureValue mustBe((None, Some(true), 2))
    }

    "srandmembers" in {
      val key = getKey()
      val res = for {
        _ <- client.sadd(key, val1, val2)
        x <- client.srandmember(key, 2)
        z <- client.scard(key)
      }yield{
          (x.size, z)
        }

      res.futureValue mustBe((2, 2))
    }

    "srem" in {
      val key = getKey()
      val res = for {
        _ <- client.sadd(key, val1, val2, val3)
        x <- client.srem(key, val1, val2)
        y <- client.scard(key)
      } yield {
          (x, y)
        }

      res.futureValue must be ((2, 1))
    }

    "sunion" in {
      val key1 = getKey()
      val key2 = getKey()
      val res = for {
        _ <- client.sadd(key1, val1)
        _ <- client.sadd(key2, val1, val2)
        x <- client.sunion(key1, key2)
      } yield {
          x
        }

      res.futureValue must be (Set(val1,val2))
    }

    "sunionstore" in {
      val key1 = getKey()
      val key2 = getKey()
      val key3 = getKey()
      val res = for {
        _ <- client.sadd(key1, val1, val3)
        _ <- client.sadd(key2, val1, val2)
        x <- client.sunionstore(key3, key1, key2)
        y <- client.smembers(key3)
      } yield {
          (x, y)
        }

      res.futureValue must be ((3,Set(val1, val2, val3)))
    }

  }

}
