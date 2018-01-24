import java.util.concurrent.ConcurrentHashMap

import akka.util.ByteString
import colossus.core.{ServerContext, ServerRef}
import colossus.protocols.redis.{BulkReply, Command, NilReply, Redis, StatusReply}
import colossus.protocols.redis.server.{Initializer, RedisServer}
import colossus.testkit.ServiceSpec

// #example
class AnyServiceSpec extends ServiceSpec[Redis] {

  val fakeDd = new ConcurrentHashMap[String, String]()

  override def service: ServerRef = {
    RedisServer.start("example-server", 9123) { initContext =>
      new Initializer(initContext) {
        override def onConnect: ServerContext => MyRequestHandler = { serverContext =>
          new MyRequestHandler(serverContext, fakeDd)
        }
      }
    }
  }

  "My request handler" must {

    "set can set a key value" in {
      val request  = Command("SET", "name", "ben")
      val response = StatusReply("OK")

      assert(fakeDd.isEmpty)
      expectResponse(request, response)
      assert(fakeDd.size() == 1)
    }

    "get can get a value" in {
      val request  = Command("GET", "name")
      val response = BulkReply(ByteString("ben"))

      expectResponse(request, response)
    }

    "get return nil when no value" in {
      val request = Command("GET", "age")

      expectResponseType[NilReply.type](request)
    }

  }
}
// #example
