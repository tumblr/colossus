package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.protocols.memcache.MemcacheClient
import colossus.protocols.memcache.MemcacheReply._
import colossus.protocols.memcache.{MemcacheCommand, MemcacheReply}
import colossus.service.{AsyncServiceClient, ClientConfig}
import colossus.testkit.ColossusSpec
import org.scalatest.concurrent.ScalaFutures

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
/*
Please be aware when running this test, that if you run it on a machine with a memcached server
running on 11211, it will begin to interact with it.  Be mindful if you are running this on a server
which has data you care about.
This test runs by hitting the REST endpoints exposed by teh TestMemcachedServer, which just proxies directly to
a Memcached client, which communicates with memcached
 */
class MemcacheITSpec extends ColossusSpec with ScalaFutures{

  type AsyncMemacheClient = AsyncServiceClient[MemcacheCommand, MemcacheReply]

  implicit val sys = IOSystem("test-system", 2)

  val client = MemcacheClient.asyncClient(ClientConfig(new InetSocketAddress("localhost", 11211), 1.second, "memcache"))

  val usedKeys = scala.collection.mutable.HashSet[ByteString]()

  implicit val ec = sys.actorSystem.dispatcher

  override def afterAll() {
    val f: Future[mutable.HashSet[Boolean]] = Future.sequence(usedKeys.map(client.delete))
    f.futureValue must be (mutable.HashSet(true, false)) //some keys are deleted by the tests.
    super.afterAll()
  }
  
  val mValue = ByteString("value")
  "Memcached client" should {
    "add" in {
      val key = getKey("colITAdd")
      val res = for {
        x <- client.add(key, mValue)
        y <- client.get(key)
        z <- client.add(key, mValue)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((true, Some(Value(ByteString("colITAdd"), mValue, 0)), false))
    }

    "append" in {
      val key = getKey("colITAppend")
      val res = for {
        w <- client.append(key, ByteString("valueA"))
        x <- client.add(key, mValue)
        y <- client.append(key, ByteString("valueA"))
        z <- client.get(key)
      } yield {
          (w,x,y,z)
        }

      res.futureValue must be ((false, true, true, Some(Value(ByteString("colITAppend"), ByteString("valuevalueA"), 0))))
    }

    "decr" in {
      val key = getKey("colITDecr")

      val res = for {
        x <- client.decr(key, 1)
        y <- client.add(key, ByteString("2"))
        z <- client.decr(key, 1)
      } yield {
          (x,y,z)
        }

      res.futureValue must be((None, true, Some(1)))

    }

    "delete" in {
      val key = getKey("colITDel")
      val res = for {
        x <- client.add(key, mValue)
        y <- client.delete(key)
        z <- client.delete(key)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((true, true, false))
    }

    "get multiple keys" in {
      val keyA = getKey("colITMGetA")
      val keyB = getKey("colITMGetB")
      val expectedMap = Map(ByteString("colITMGetA")->Value(ByteString("colITMGetA"), mValue, 0), ByteString("colITMGetB")->Value(ByteString("colITMGetB"), mValue, 0))

      val res = for {
        _ <- client.add(keyA, mValue)
        _ <- client.add(keyB, mValue)
        x <- client.getAll(keyA, keyB)
      } yield {
          x
        }

      res.futureValue must be (expectedMap)
    }

    "incr" in {
      val key = getKey("colITIncr")
      val res = for {
        x <- client.incr(key, 1)
        y <- client.add(key, ByteString("2"))
        z <- client.incr(key, 1)
      } yield {
          (x,y,z)
        }

      res.futureValue must be ((None, true, Some(3)))
    }

    "prepend" in {
      val key = getKey("colITPrepend")
      val res = for {
        w <- client.prepend(key, ByteString("valueA"))
        x <- client.add(key, mValue)
        y <- client.prepend(key, ByteString("valueP"))
        z <- client.get(key)
      } yield {
          (w,x,y,z)
        }

      res.futureValue must be(false, true, true, Some(Value(ByteString("colITPrepend"), ByteString("valuePvalue"), 0)))
    }

    "replace" in {
      val key = getKey("colITReplace")
      val res = for {
        w <- client.replace(key, mValue)
        x <- client.add(key, mValue)
        y <- client.replace(key, ByteString("newValue"))
        z <- client.get(key)
      } yield {
          (w,x,y,z)
        }
    }

    "set" in {
      val key = getKey("colITSet")
      val res = for {
        w <- client.set(key, mValue)
        x <- client.get(key)
        y <- client.set(key, ByteString("newValue"))
        z <- client.get(key)
      } yield {
          (w,x,y,z)
        }

      res.futureValue must be(true, Some(Value(ByteString("colITSet"), ByteString("value"), 0)), true, Some(Value(ByteString("colITSet"), ByteString("newValue"), 0)))
    }

    "touch" in {
      val key = getKey("colITTouch")
      val res = for {
        x <- client.touch(key, 100)
        y <- client.set(key, mValue)
        z <- client.touch(key, 100)
      } yield {
          (x,y,z)
        }
      res.futureValue must be (false, true, true)
    }
  }

  def getKey(key: String): ByteString = {
    val bKey = ByteString(key)
    usedKeys += bKey
    bKey
  }
}
