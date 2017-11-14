import akka.util.ByteString
import colossus.core.{ServerContext, ServerRef, WorkerRef}
import colossus.protocols.http.{Http, HttpCodes, HttpRequest, HttpServer, Initializer}
import colossus.protocols.memcache.{Memcache, MemcacheClient, MemcacheCommand, MemcacheReply}
import colossus.protocols.redis.{BulkReply, Command, NilReply, Redis, RedisClient, Reply}
import colossus.service.Callback
import colossus.testkit.{HttpServiceSpec, MockSender}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import scala.util.Success

// #example1
class AnyClientSpec extends HttpServiceSpec {

  val basicRedis: Command => Callback[Reply] = MockSender.mockResponse[Redis](
    Map(
      Command("GET", "ben") -> Success(BulkReply(ByteString("1"))),
      Command("GET", "bob") -> Success(NilReply),
      Command("GET", "jen") -> Success(NilReply)
    )
  )

  val basicMemcache: MemcacheCommand => Callback[MemcacheReply] = MockSender.mockResponse[Memcache](
    Map(
      MemcacheCommand.Get(ByteString("bob")) -> Success(MemcacheReply.Value(ByteString("ben"), ByteString("2"), 0)),
      MemcacheCommand.Get(ByteString("jen")) -> Success(MemcacheReply.NoData)
    )
  )

  override def service: ServerRef = {
    HttpServer.start("example", 9123) { initContext =>
      implicit val workerRef: WorkerRef = initContext.worker

      val redis    = Redis.client(MockSender[Redis, Callback](basicRedis))
      val memcache = Memcache.client(MockSender[Memcache, Callback](basicMemcache))

      new Initializer(initContext) {
        override def onConnect: ServerContext => AnyClientHandler = { serverContext =>
          new AnyClientHandler(serverContext, redis, memcache)
        }
      }
    }
  }

  "Any client handler" must {
    "return valid redis value" in {
      expectCodeAndBody(HttpRequest.get("/data/ben"), HttpCodes.OK, "The key ben has value 1")
    }

    "return valid memcache value" in {
      expectCodeAndBody(HttpRequest.get("/data/bob"), HttpCodes.OK, "The key bob has value 2")
    }

    "return no value for unknown key" in {
      expectCodeAndBody(HttpRequest.get("/data/jen"), HttpCodes.OK, "The key jen has no value")
    }
  }
}
// #example1

// #example2
class AnyClientSpec2 extends HttpServiceSpec with MockitoSugar {

  val redis: RedisClient[Callback]       = mock[RedisClient[Callback]]
  val memcache: MemcacheClient[Callback] = mock[MemcacheClient[Callback]]

  override def service: ServerRef = {
    HttpServer.start("example", 9123) { initContext =>
      implicit val workerRef: WorkerRef = initContext.worker

      new Initializer(initContext) {
        override def onConnect: ServerContext => AnyClientHandler = { serverContext =>
          new AnyClientHandler(serverContext, redis, memcache)
        }
      }
    }
  }

  "Any client handler" must {
    "return valid redis value" in {
      when(redis.get(ByteString("ben"))).thenReturn(Callback.successful(ByteString("1")))
      expectCodeAndBody(HttpRequest.get("/data/ben"), HttpCodes.OK, "The key ben has value 1")
    }

    "return valid memcache value" in {
      when(redis.get(ByteString("bob"))).thenReturn(Callback.failed(new Exception("not here")))
      when(memcache.get(ByteString("bob"))).thenReturn(
        Callback.successful(Some(MemcacheReply.Value(ByteString("ben"), ByteString("2"), 0)))
      )
      expectCodeAndBody(HttpRequest.get("/data/bob"), HttpCodes.OK, "The key bob has value 2")
    }

    "return no value for unknown key" in {
      when(redis.get(ByteString("jen"))).thenReturn(Callback.failed(new Exception("not here")))
      when(memcache.get(ByteString("jen"))).thenReturn(Callback.successful(None))
      expectCodeAndBody(HttpRequest.get("/data/jen"), HttpCodes.OK, "The key jen has no value")
    }
  }
}
// #example2
