package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.metrics.MetricSystem
import colossus.protocols.redis._
import colossus.service.ClientConfig
import colossus.testkit.ColossusSpec
import org.scalatest.concurrent.{ScalaFutures, ScaledTimeSpans}
import org.scalatest.time.Span
import org.scalatest.time._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

abstract class BaseRedisITSpec extends ColossusSpec with ScalaFutures with ScaledTimeSpans {

  val metricSystem =  MetricSystem("test-system")

  implicit val sys: IOSystem        = IOSystem("test-system", Some(2), metricSystem)
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val pc: PatienceConfig   = PatienceConfig(Span(1, Minutes))

  protected var nextId : Integer = 1

  def keyPrefix : String

  val client = Redis.futureClient(ClientConfig(new InetSocketAddress("localhost", 6379), 1.second, "redis"))

  val usedKeys = scala.collection.mutable.HashSet.empty[ByteString]

  override def afterAll() {
    val f: Future[Long] = client.del(usedKeys.toSeq : _*)
    f.futureValue
    super.afterAll()
  }

  // this defaults to the keyPrefix, and the only reason this takes a parameter is so
  // that i can override it in each test(look at the 'keys' command test in RedisITSpec
  def getKey(prefix : String = keyPrefix) : ByteString = {
    nextId += 1
    val bKey = ByteString(prefix + nextId)
    usedKeys += bKey
    bKey
  }

}
