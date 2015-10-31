package colossus

import akka.util.ByteString

class RedisZSetITSpec extends BaseRedisITSpec {

  val keyPrefix = "colossusITZSet"

  val val1  = ByteString("value1")
  val val2  = ByteString("value2")
  val val3  = ByteString("value3")
  val val4  = ByteString("value4")

  "Redis zset commands" should {

    "zadd" in {
      val key = getKey()
      val res = for {
        x <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        y <- client.zcard(key)
      } yield {
          (x,y)
        }

      res.futureValue must be((2D, 2))
    }

    "zcard" in {
      val key = getKey()
      val res = for {
        x <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        y <- client.zcard(key)
      } yield {
          (x,y)
        }

      res.futureValue must be((2D, 2))
    }

    "zcount" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        x <- client.zcount(key, None, Some(1.0))
      } yield{
          x
        }

      res.futureValue must be (1)
    }

    "zincrby" in {
      val key = getKey()
      val res = for {
        x <- client.zincrby(key, 5.0, val1)
        y <- client.zincrby(key, 5.0, val1)
      }yield {
          (x,y)
        }

      res.futureValue must be((5.0D, 10D))
    }

    "zinterstore" in {
      val key = getKey()
      val key2 = getKey()
      val destKey = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        _ <- client.zadd(key2, ByteString("1"), val1, ByteString("2"), val2)
        x <- client.zinterstore(destKey, 2, key, key2)
      } yield {
          x
        }

      res.futureValue must be (2)

    }

    "zinterstore with arguments" in {
      val key = getKey()
      val key2 = getKey()
      val destKey = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        _ <- client.zadd(key2, ByteString("5"), val1, ByteString("10"), val2)
        x <- client.zinterstore(destKey, 2, key, key2, ByteString("WEIGHTS"), ByteString("2"), ByteString("3"), ByteString("AGGREGATE"), ByteString("MAX"))
        y <- client.zscore(destKey, val1)
        z <- client.zscore(destKey, val2)
      } yield {
          (x, y, z)
        }

      res.futureValue must be ((2, 15D, 30D))

    }

    "zlexcount" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("1"), val2)
        x <- client.zlexcount(key)
      } yield {
          x
        }

      res.futureValue must be (2)
    }

    "zrange" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zrange(key, 0,1)
      } yield {
          x
        }

      res.futureValue must be (Seq(val1, val2))
    }

    "zrangewithscores" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zrange(key, 0,1, ByteString("withscores"))
      } yield {
          x
        }

      res.futureValue must be (Seq(val1, ByteString("1"), val2, ByteString("2")))
    }

    "zrangebylex" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("0"), val1, ByteString("0"), val2, ByteString("0"), val3)
        x <- client.zrangebylex(key, ByteString("-"), ByteString("+"))
      } yield {
          x
        }

      res.futureValue must be (Seq(val1, val2, val3))
    }

    "zrangebylex with arguments" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("0"), val1, ByteString("0"), val2, ByteString("0"), val3)
        x <- client.zrangebylex(key, ByteString("-"), ByteString("+"), ByteString("LIMIT"), ByteString("1"), ByteString("1"))
      } yield {
          x
        }

      res.futureValue must be (Seq(val2))
    }

    "zrangebyscore" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zrangebyscore(key, ByteString("1"), ByteString("2"))
      } yield {
          x
        }

      res.futureValue must be (Seq(val1, val2))
    }

    "zrank" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zrank(key, val1)
      } yield {
          x
        }

      res.futureValue must be (0)
    }

    "zrankoption" in {
      val key = getKey()
      val res = for {
        x <- client.zrankOption(key, val1)
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        y <- client.zrankOption(key, val3)
        z <- client.zrankOption(key, val1)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((None, None, Some(0)))
    }

    "zrem" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        x <- client.zrem(key, val1, val3)
      }yield {
          x
        }

      res.futureValue must be (1)
    }

    "zremrangebylex" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), ByteString("a"), ByteString("1"), ByteString("b"), ByteString("1"), ByteString("c"))
        x <- client.zremrangebylex(key, ByteString("[a"), ByteString("[b"))
        y <- client.zcard(key)
      }yield {
          (x,y)
        }

      res.futureValue must be ((2, 1))
    }

    "zremrangebyrank" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), ByteString("a"), ByteString("2"), ByteString("b"), ByteString("3"), ByteString("c"))
        x <- client.zremrangebyrank(key, 0, 1)
        y <- client.zcard(key)
      }yield {
          (x,y)
        }

      res.futureValue must be ((2, 1))
    }

    "zremrangebyscore" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), ByteString("a"), ByteString("2"), ByteString("b"), ByteString("3"), ByteString("c"))
        x <- client.zremrangebyscore(key, ByteString("-inf"), ByteString("(2"))
        y <- client.zcard(key)
      }yield {
          (x,y)
        }

      res.futureValue must be ((1,2))
    }

    "zrevrange" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zrevrange(key, 1,2)
      } yield {
          x
        }

      res.futureValue must be (Seq(val2, val1))
    }

    "zrevrangebylex" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("0"), val1, ByteString("0"), val2, ByteString("0"), val3)
        x <- client.zrevrangebylex(key, ByteString("+"), ByteString("-"))
      } yield {
          x
        }

      res.futureValue must be (Seq(val3, val2, val1))
    }

    "zrevrangebyscore" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zrevrangebyscore(key, ByteString("2"), ByteString("1"))
      } yield {
          x
        }

      res.futureValue must be (Seq(val2, val1))
    }

    "zrevrank" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zrevrank(key, val1)
      } yield {
          x
        }

      res.futureValue must be (2)
    }

    "zrevrankoption" in {
      val key = getKey()
      val res = for {
        x <- client.zrevrankOption(key, val1)
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        y <- client.zrevrankOption(key, val3)
        z <- client.zrevrankOption(key, val1)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((None, None, Some(1)))
    }

    "zscore" in {
      val key = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        x <- client.zscore(key, val1)
      } yield {
          x
        }

      res.futureValue must be (1D)
    }

    "zscoreoption" in {
      val key = getKey()
      val res = for {
        x <- client.zscoreOption(key, val1)
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2)
        y <- client.zscoreOption(key, val3)
        z <- client.zscoreOption(key, val1)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((None, None, Some(1)))
    }

    "zunionstore" in {
      val key = getKey()
      val key2 = getKey()
      val destKey = getKey()
      val res = for {
        _ <- client.zadd(key, ByteString("1"), val1, ByteString("2"), val2, ByteString("3"), val3)
        _ <- client.zadd(key2, ByteString("1"), val1, ByteString("2"), val2)
        x <- client.zunionstore(destKey, 2, key, key2)
      } yield {
          x
        }

      res.futureValue must be (3)

    }
  }

}
