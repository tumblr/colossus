package colossus

import akka.util.ByteString

class RedisListITSpec extends BaseRedisITSpec{

  val keyPrefix = "colossusITList"
  val lValue = ByteString("value")
  val lValue2 = ByteString("value2")

  "Redis List commands" should {

    "lindex" in {
      val key = getKey()

      val res = for {
        w <- client.lindexOption(key, 0) //non existent key check
        _ <- client.lpush(key, lValue) //create list
        x <- client.lindex(key, 0) //should get lValue
        y <- client.lindexOption(key, 0) //should get lValue as option
        z <- client.lindexOption(key, 1) //out of bounds should be None
      } yield {
          (w,x,y,z)
        }

      res.futureValue must be ((None, lValue, Some(lValue), None))
    }

    "linsert" in {
      val key = getKey()

      val res = for {
        _ <- client.lpush(key, lValue) //create list
        w <- client.linsertBefore(key, lValue, lValue2) //insert value2 before value1
        x <- client.lindex(key, 0) //should be lValue2
      }yield {
          (w,x)
        }

      res.futureValue must be ((2, lValue2))
    }

    "llen" in {
      val key = getKey()

      val res = for {
        w <- client.llen(key) //non existent list should be 0
        _ <- client.lpush(key, lValue, lValue2) //populate list
        x <- client.llen(key)
      }yield {
          (w,x)
        }

      res.futureValue mustBe ((0,2))
    }

    "lpop" in {
      val key = getKey()

      val res = for {
        w <- client.lpopOption(key) //shouldn't work on an empty key
        _ <- client.lpush(key, lValue, lValue2)
        x <- client.lpop(key) //should be lValue2, see redis doc on lpush
      } yield {
          (w,x)
        }

      res.futureValue must be((None, lValue2))

    }

    "lpush" in {
      val key = getKey()

      val res = for {
        w <- client.lpush(key, lValue, lValue2) //lpush on non existent list should create
        x <- client.lpush(key, lValue2, lValue) //push some more, returns length
        y <- client.lrange(key, 0,3) //push some more, returns length
      } yield {
          (w,x,y)
        }

      res.futureValue must be((2,4, Seq(lValue, lValue2, lValue2, lValue)))
    }

    "lpushx" in {
      val key = getKey()

      val res = for {
        w <- client.lpushx(key, lValue) //lpushx on non existent list should not create
        x <- client.lpush(key, lValue) //push stuff
        y <- client.lpushx(key, lValue2) //pushx should work now
        z <- client.lrange(key, 0,1) //push some more, returns length
      } yield {
          (w,x,y,z)
        }

      res.futureValue must be((0,1,2,Seq(lValue2, lValue)))
    }

    "lrange" in {
      val key = getKey()

      val res = for {
        w <- client.lrange(key, 0, 0) //should be empty
        _ <- client.lpush(key, lValue, lValue2, lValue) //push stuff
        x <- client.lrange(key, 0,1) //get list
      } yield {
          (w,x)
        }

      res.futureValue must be((Nil, Seq(lValue, lValue2)))
    }

    "lrem" in {
      val key = getKey()

      val res = for {
        _ <- client.lpush(key, lValue, lValue, lValue, lValue)
        x <- client.lrem(key, 2, lValue) //this should remove 2 copies of lValue
        y <- client.llen(key) //should now be 2
      }yield {
          (x,y)
        }

      res.futureValue must be ((2,2))
    }

    "lset" in {
      val key = getKey()

      val res = for {
        _ <- client.lpush(key, lValue, lValue, lValue)
        _ <- client.lset(key, 1, lValue2)
        x <- client.lindex(key, 1)

      }yield{
          x
        }

      res.futureValue must be (lValue2)
    }


    "ltrim" in {
      val key = getKey()

      val res = for {
        _ <- client.lpush(key, lValue, lValue, lValue, lValue)
        _ <- client.ltrim(key, 0, 2) //trim the list to 3 elements
        x <- client.llen(key)

      }yield{
          x
        }

      res.futureValue must be (3)
    }

    "rpop" in {
      val key = getKey()

      val res = for {
        w <- client.rpopOption(key) //should be None, list doesn't exist
        _ <- client.lpush(key, lValue, lValue, lValue, lValue2) //create list
        x <- client.rpop(key) //pop last element, lValue
        y <- client.llen(key) //should be 3

      }yield{
          (w,x,y)
        }

      res.futureValue must be ((None, lValue, 3))
    }

    "rpoplpush" in {
      val key = getKey()
      val destKey = getKey()

      val res = for {
        w <- client.rpoplpushOption(key, destKey) //should be None, source key doesn't exist
        _ <- client.lpush(key, lValue, lValue, lValue, lValue2) //create source list
        x <- client.rpoplpush(key, destKey) //x should be lValue
        y <- client.llen(key) //should be 3 , since item was popped
        z <- client.llen(destKey) //should be 1 since item was added

      }yield{
          (w,x,y,z)
        }

      res.futureValue must be ((None, lValue, 3, 1))
    }

    "rpush" in {
      val key = getKey()

      val res = for {
        w <- client.rpush(key, lValue, lValue2) //rpush on non existent list should create
        x <- client.rpush(key, lValue2, lValue) //push some more, returns length
        z <- client.lrange(key, 0, 3) //get the list to make sure the push was at the tail
      } yield {
          (w,x,z)
        }

      res.futureValue must be((2,4, Seq(lValue,lValue2, lValue2, lValue)))
    }

    "rpushx" in {
      val key = getKey()

      val res = for {
        w <- client.rpushx(key, lValue) //rpushx on non existent list should not create
        x <- client.rpush(key, lValue) //push stuff
        y <- client.rpushx(key, lValue2) //pushx should now work
        z <- client.lrange(key, 0,1) //get list
      } yield {
          (w,x,y,z)
        }

      res.futureValue must be((0,1,2,Seq(lValue, lValue2)))
    }
  }

}
