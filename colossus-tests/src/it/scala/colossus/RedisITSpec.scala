package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.protocols.redis.Commands._
import colossus.protocols.redis._
import colossus.service.{AsyncServiceClient, ClientConfig}
import colossus.testkit.ColossusSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
/*
Please be aware when running this test, that if you run it on a machine with a redis server
running on 6379, it will begin to interact with it.  Be mindful if you are running this on a server
which has data you care about.
This test runs by hitting the REST endpoints exposed by teh TestRedisServer, which just proxies directly to
a Redis client, which communicates with redis
 */
class RedisITSpec extends ColossusSpec with ScalaFutures{

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val sys = IOSystem("test-system", 2)

  type AsyncRedisClient = AsyncServiceClient[Command, Reply]

  val client = AsyncServiceClient[Redis](ClientConfig(new InetSocketAddress("localhost", 6379), 1.second, "redis"))

  val usedKeys = scala.collection.mutable.MutableList[ByteString]()

  override def afterAll() {
    usedKeys.foreach(key => del(client, key))
    super.afterAll()
  }

  "Redis Client" should {

    "del" in {
      val delKey = getKey("colITDel")
      set(client, delKey, ByteString("value")).futureValue must be (true)
      del(client, delKey).futureValue must be (1)
    }

    "existence" in {
      val exKey = getKey("colITEx")
      exists(client, exKey).futureValue must be (false)
      set(client, exKey, ByteString("value")).futureValue must be (true)
      exists(client, exKey).futureValue must be (true)
    }

    "set && get" in {
      val setKey = getKey("colITSet")
      set(client, setKey, ByteString("value")).futureValue must be (true)
      get(client, setKey).futureValue must be (ByteString("value"))
    }

    "setnx" in {
      val setnxKey = getKey("colITSetnx")
      setnx(client, setnxKey, ByteString("value")).futureValue must be (true)
      get(client, setnxKey).futureValue must be (ByteString("value"))
      setnx(client, setnxKey, ByteString("value")).futureValue must be (false)
    }

    "setex && ttl" in {
      val setexKey = getKey("colITSetex")
      setex(client, setexKey, ByteString("value"), 10.seconds).futureValue must be (true)
      get(client, setexKey).futureValue must be (ByteString("value"))
      ttl(client, setexKey).futureValue must be (Some(10))
    }

    "strlen" in {
      val strlenKey = getKey("colITStrlen")
      set(client, strlenKey, ByteString("value"))
      strlen(client, strlenKey).futureValue must be (Some(5))
    }
  }

  //these might be somethign to be moved onto the client itself at some point.
  //Especially if we unify the interfaces between Callback/Futures using higher kinded types.
  def del(client : AsyncRedisClient, key : ByteString) : Future[Long] = {
    client.send(Del(key)).flatMap {
      case IntegerReply(x) => Future.successful(x)
      case x => Future.failed(new Exception(s"unexpected response $x when deleting $key"))
    }
  }
  def exists(client : AsyncRedisClient, key : ByteString) : Future[Boolean] = {
    client.send(Exists(key)).flatMap {
      case IntegerReply(x) => Future.successful(x == 1)
      case x => Future.failed(new Exception(s"unexpected response $x when checking for existence of $key"))
    }
  }
  def get(client : AsyncRedisClient, key : ByteString) = {
    client.send(Get(key)).flatMap {
      case BulkReply(x) => Future.successful(x)
      case x => Future.failed(new Exception(s"une xpected response $x when getting $key"))
    }
  }

  def set(client : AsyncRedisClient, key : ByteString, value : ByteString) = {
    client.send(Set(key, value)).flatMap {
      case StatusReply(x) => Future.successful(true)
      case x => Future.failed(new Exception(s"unexpected response $x when setting $key and $value"))
    }
  }
  def setnx(client : AsyncRedisClient, key : ByteString, value : ByteString) = {
    client.send(Setnx(key, value)).flatMap {
      case IntegerReply(x) => Future.successful(x == 1)
      case x => Future.failed(new Exception(s"unexpected response $x when setnx $key and $value"))
    }
  }
  def setex(client : AsyncRedisClient, key : ByteString, value : ByteString, ttl : FiniteDuration) = {
    client.send(Setex(key, value, ttl)).flatMap {
      case StatusReply(x) => Future.successful(true)
      case x => Future.failed(new Exception(s"unexpected response $x when settex $key and  $value with $ttl"))
    }
  }
  def strlen(client : AsyncRedisClient, key : ByteString) : Future[Option[Long]] = {
    client.send(Strlen(key)).flatMap {
      case IntegerReply(x) if x > 0 => Future.successful(Some(x))
      case IntegerReply(x) => Future.successful(None)
      case x => Future.failed(new Exception(s"unexpected response $x when strlen $key"))
    }
  }
  def ttl(client : AsyncRedisClient, key : ByteString) : Future[Option[Long]] = {
    client.send(Ttl(key)).flatMap {
      case IntegerReply(x) => Future.successful(Some(x))
      case StatusReply(x) => Future.successful(None)
      case x => Future.failed(new Exception(s"unexpected response $x when ttl $key"))
    }
  }

  def getKey(key: String): ByteString = {
    val bKey = ByteString(key)
    usedKeys += bKey
    bKey
  }
}
