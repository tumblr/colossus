package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.metrics.MetricSystem
import colossus.protocols.redis.{Command, RedisClient, Reply}
import colossus.service.{AsyncServiceClient, ClientConfig}
import colossus.testkit.ColossusSpec
import org.scalatest.concurrent.{ScaledTimeSpans, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.time._

import scala.concurrent.Future
import scala.concurrent.duration._

abstract class BaseRedisITSpec extends ColossusSpec with ScalaFutures with ScaledTimeSpans {

  implicit val sys = IOSystem("test-system", Some(2), MetricSystem.deadSystem)

  implicit val ec = system.dispatcher

  protected var nextId : Integer = 1

  implicit val defaultPatience = PatienceConfig(Span(1, Minutes))

  def keyPrefix : String

  type AsyncRedisClient = AsyncServiceClient[Command, Reply]

  val client = RedisClient.asyncClient(ClientConfig(new InetSocketAddress("localhost", 6379), 1.second, "redis"))

  val usedKeys = scala.collection.mutable.HashSet[ByteString]()

  override def afterAll() {
    val f: Future[Long] = client.del(usedKeys.toSeq : _*)
    val b  = f.futureValue
    super.afterAll()
  }

  //this defaults to the keyPrefix, and the only reason this takes a parameter is so
  // that i can override it in each test(look at the 'keys' command test in RedisITSpec
  def getKey(prefix : String = keyPrefix) : ByteString = {
    nextId += 1
    val bKey = ByteString(prefix + nextId)
    usedKeys += bKey
    bKey
  }

}
