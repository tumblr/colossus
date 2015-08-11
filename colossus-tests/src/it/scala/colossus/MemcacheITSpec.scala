package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.protocols.MemcacheClient
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

  override def afterAll() {
    implicit val ec = sys.actorSystem.dispatcher
    val f: Future[mutable.HashSet[Boolean]] = Future.sequence(usedKeys.map(client.delete))
    f.futureValue must be (mutable.HashSet(true, false)) //some keys are deleted by the tests.
    super.afterAll()
  }
  
  val mValue = ByteString("value")
  "Memcached client" should {
    "add" in {
      val key = getKey("colITAdd")
      client.add(key, mValue).futureValue must be (true)
      client.get(key).futureValue must be (Map("colITAdd"->Value("colITAdd", mValue, 0)))
      client.add(key, mValue).futureValue must be (false)
    }

    "append" in {
      val key = getKey("colITAppend")
      client.append(key, ByteString("valueA")).futureValue must be (false)
      client.add(key, mValue).futureValue must be (true)
      client.append(key, ByteString("valueA")).futureValue must be (true)
      client.get(key).futureValue must be (Map("colITAppend"->Value("colITAppend", ByteString("valuevalueA"), 0)))
    }

    "decr" in {
      val key = getKey("colITDecr")
      client.decr(key, 1).futureValue must be (None)
      client.add(key, ByteString("2")).futureValue must be (true)
      client.decr(key, 1).futureValue must be (Some(1))
    }

    "delete" in {
      val key = getKey("colITDel")
      client.add(key, mValue).futureValue must be (true)
      client.delete(key).futureValue must be (true)
      client.delete(key).futureValue must be (false)
    }

    "get multiple keys" in {
      val keyA = getKey("colITMGetA")
      val keyB = getKey("colITMGetB")
      val expectedMap = Map("colITMGetA"->Value("colITMGetA", mValue, 0), "colITMGetB"->Value("colITMGetB", mValue, 0))
      client.add(keyA, mValue).futureValue must be (true)
      client.add(keyB, mValue).futureValue must be (true)
      client.get(keyA, keyB).futureValue mustBe expectedMap
    }

    "incr" in {
      val key = getKey("colITIncr")
      client.incr(key, 1).futureValue must be (None)
      client.add(key, ByteString("2")).futureValue must be (true)
      client.incr(key, 1).futureValue must be (Some(3))
    }

    "prepend" in {
      val key = getKey("colITPrepend")
      client.prepend(key, ByteString("valueA")).futureValue must be (false)
      client.add(key, mValue).futureValue must be (true)
      client.prepend(key, ByteString("valueP")).futureValue must be (true)
      client.get(key).futureValue must be (Map("colITPrepend"->Value("colITPrepend", ByteString("valuePvalue"), 0)))
    }

    "replace" in {
      val key = getKey("colITReplace")
      client.replace(key, mValue).futureValue must be (false)
      client.add(key, mValue).futureValue must be (true)
      client.replace(key, ByteString("newValue")).futureValue must be (true)
      client.get(key).futureValue must be (Map("colITReplace"->Value("colITReplace", ByteString("newValue"), 0)))
    }

    "set" in {
      val key = getKey("colITSet")
      client.set(key, mValue).futureValue must be (true)
      client.get(key).futureValue must be (Map("colITSet"->Value("colITSet", ByteString("value"), 0)))
      client.set(key, ByteString("newValue")).futureValue must be (true)
      client.get(key).futureValue must be (Map("colITSet"->Value("colITSet", ByteString("newValue"), 0)))
    }

    "touch" in {
      val key = getKey("colITTouch")
      client.touch(key, 100).futureValue must be (false)
      client.set(key, mValue).futureValue must be (true)
      client.touch(key, 100).futureValue must be (true)
    }
  }

  def getKey(key: String): ByteString = {
    val bKey = ByteString(key)
    usedKeys += bKey
    bKey
  }
}
