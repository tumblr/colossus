package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.protocols.memcache.MemcacheCommand._
import colossus.protocols.memcache.MemcacheReply._
import colossus.protocols.memcache.{Memcache, MemcacheCommand, MemcacheReply, MemcachedKey}
import colossus.service.{AsyncServiceClient, ClientConfig}
import colossus.testkit.ColossusSpec
import org.scalatest.concurrent.ScalaFutures

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

  import scala.concurrent.ExecutionContext.Implicits.global

  val client = AsyncServiceClient[Memcache](ClientConfig(new InetSocketAddress("localhost", 11211), 1.second, "memcache"))
  val usedKeys = scala.collection.mutable.MutableList[ByteString]()

  override def afterAll() {
    usedKeys.foreach(key => delete(client, key))
    super.afterAll()
  }

  val mValue = ByteString("value")
  "Memcached client" should {
    "add" in {
      val key = getKey("colITAdd")
      add(client, key, mValue).futureValue must be (true)
      get(client, key).futureValue must be (Map("colITAdd"->Value("colITAdd", mValue, 0)))
      add(client, key, mValue).futureValue must be (false)
    }

    "append" in {
      val key = getKey("colITAppend")
      append(client, key, ByteString("valueA")).futureValue must be (false)
      add(client, key, mValue).futureValue must be (true)
      append(client, key, ByteString("valueA")).futureValue must be (true)
      get(client, key).futureValue must be (Map("colITAppend"->Value("colITAppend", ByteString("valuevalueA"), 0)))
    }

    "decr" in {
      val key = getKey("colITDecr")
      decr(client, key, 1).futureValue must be (None)
      add(client, key, ByteString("2")).futureValue must be (true)
      decr(client, key, 1).futureValue must be (Some(1))
    }

    "delete" in {
      val key = getKey("colITDel")
      add(client, key, mValue).futureValue must be (true)
      delete(client, key).futureValue must be (true)
      delete(client, key).futureValue must be (false)
    }

    "get multiple keys" in {
      val keyA = getKey("colITMGetA")
      val keyB = getKey("colITMGetB")
      val expectedMap = Map("colITMGetA"->Value("colITMGetA", mValue, 0), "colITMGetB"->Value("colITMGetB", mValue, 0))
      add(client, keyA, mValue).futureValue must be (true)
      add(client, keyB, mValue).futureValue must be (true)
      get(client, keyA, keyB).futureValue mustBe expectedMap
    }

    "incr" in {
      val key = getKey("colITIncr")
      incr(client, key, 1).futureValue must be (None)
      add(client, key, ByteString("2")).futureValue must be (true)
      incr(client, key, 1).futureValue must be (Some(3))
    }

    "prepend" in {
      val key = getKey("colITPrepend")
      prepend(client, key, ByteString("valueA")).futureValue must be (false)
      add(client, key, mValue).futureValue must be (true)
      prepend(client, key, ByteString("valueP")).futureValue must be (true)
      get(client, key).futureValue must be (Map("colITPrepend"->Value("colITPrepend", ByteString("valuePvalue"), 0)))
    }

    "replace" in {
      val key = getKey("colITReplace")
      replace(client, key, mValue).futureValue must be (false)
      add(client, key, mValue).futureValue must be (true)
      replace(client, key, ByteString("newValue")).futureValue must be (true)
      get(client, key).futureValue must be (Map("colITReplace"->Value("colITReplace", ByteString("newValue"), 0)))
    }

    "set" in {
      val key = getKey("colITSet")
      set(client, key, mValue).futureValue must be (true)
      get(client, key).futureValue must be (Map("colITSet"->Value("colITSet", ByteString("value"), 0)))
      set(client, key, ByteString("newValue")).futureValue must be (true)
      get(client, key).futureValue must be (Map("colITSet"->Value("colITSet", ByteString("newValue"), 0)))
    }

    "touch" in {
      val key = getKey("colITTouch")
      touch(client, key, 100).futureValue must be (false)
      set(client, key, mValue).futureValue must be (true)
      touch(client, key, 100).futureValue must be (true)
    }
  }

  //potential client interface (with some refinements)
  def add(client : AsyncMemacheClient, key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) = {
    client.send(Add(MemcachedKey(key), value, ttl, flags)).flatMap {
      case Stored => Future.successful(true)
      case NotStored => Future.successful(false)
      case x => Future.failed(new Exception(s"unexpected response $x when adding $key and $value"))
    }
  }

  def append(client : AsyncMemacheClient, key : ByteString, value : ByteString) = {
    client.send(Append(MemcachedKey(key), value)).flatMap {
      case Stored => Future.successful(true)
      case NotStored => Future.successful(false)
      case x => Future.failed(new Exception(s"unexpected response $x when appending $key and $value"))
    }
  }

  def decr(client : AsyncMemacheClient, key : ByteString, value : Long) : Future[Option[Long]] = {
    client.send(Decr(MemcachedKey(key), value)).flatMap {
      case Counter(v) => Future.successful(Some(v))
      case NotFound => Future.successful(None)
      case x => Future.failed(new Exception(s"unexpected response $x when decr $key with $value"))
    }
  }

  def delete(client : AsyncMemacheClient, key : ByteString) = {
    client.send(Delete(MemcachedKey(key))).flatMap {
      case Deleted => Future.successful(true)
      case NotFound => Future.successful(false)
      case x => Future.failed(new Exception(s"unexpected response $x when deleting $key"))
    }
  }

  def get(client : AsyncMemacheClient, keys : ByteString*) : Future[Map[String, Value]] = {
    client.send(Get(keys.map(MemcachedKey(_)) : _*)).flatMap {
      case a : Value => Future.successful(Map(a.key->a))
      case Values(x) => Future.successful(x.map(y => y.key->y).toMap)
      case x => Future.failed(new Exception(s"unexpected response $x when getting $keys"))
    }
  }

  def incr(client : AsyncMemacheClient, key : ByteString, value : Long) = {
    client.send(Incr(MemcachedKey(key), value)).flatMap {
      case Counter(v) => Future.successful(Some(v))
      case NotFound => Future.successful(None)
      case x => Future.failed(new Exception(s"unexpected response $x when incr $key with $value"))
    }
  }

  def prepend(client : AsyncMemacheClient, key : ByteString, value : ByteString) = {
    client.send(Prepend(MemcachedKey(key), value)).flatMap {
      case Stored => Future.successful(true)
      case NotStored => Future.successful(false)
      case x => Future.failed(new Exception(s"unexpected response $x when prepending $key and $value"))
    }
  }

  def replace(client : AsyncMemacheClient, key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) = {
    client.send(Replace(MemcachedKey(key), value, ttl, flags)).flatMap {
      case Stored => Future.successful(true)
      case NotStored => Future.successful(false)
      case x => Future.failed(new Exception(s"unexpected response $x when replacing $key and $value"))
    }
  }


  def set(client : AsyncMemacheClient, key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) = {
    client.send(Set(MemcachedKey(key), value, ttl, flags)).flatMap {
      case Stored => Future.successful(true)
      case NotStored => Future.successful(false)
      case x => Future.failed(new Exception(s"unexpected response $x when setting $key and $value"))
    }
  }

  def touch(client : AsyncMemacheClient, key : ByteString, ttl : Int = 0) = {
    client.send(Touch(MemcachedKey(key), ttl)).flatMap {
      case Touched => Future.successful(true)
      case NotFound => Future.successful(false)
      case x => Future.failed(new Exception(s"unexpected response $x when touching $key with $ttl"))
    }
  }

  def getKey(key: String): ByteString = {
    val bKey = ByteString(key)
    usedKeys += bKey
    bKey
  }
}
