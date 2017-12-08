import akka.actor.ActorSystem
import akka.util.ByteString
import colossus.IOSystem
import colossus.core.{ServerContext, WorkerRef}
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.protocols.http.{Http, HttpServer, Initializer, RequestHandler}
import colossus.protocols.memcache.MemcacheReply.Value
import colossus.protocols.memcache.{Memcache, MemcacheClient}
import colossus.protocols.redis.{Redis, RedisClient}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

object AnyClientExample extends App {
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val ioSystem: IOSystem       = IOSystem()

  HttpServer.start("example", 9000) { initContext =>
    implicit val workerRef: WorkerRef = initContext.worker

    val redisClient    = Redis.client("localhost", 6379)
    val memcacheClient = Memcache.client("localhost", 11211)

    new Initializer(initContext) {
      override def onConnect: ServerContext => AnyClientHandler = { serverContext =>
        new AnyClientHandler(serverContext, redisClient, memcacheClient)
      }
    }
  }
}

// #example
class AnyClientHandler(context: ServerContext,
                       redisClient: RedisClient[Callback],
                       memcacheClient: MemcacheClient[Callback])
    extends RequestHandler(context) {
  override def handle: PartialHandler[Http] = {
    case request @ Get on Root / "data" / name =>
      redisClient
        .get(ByteString(name))
        .flatMap {
          case Some(value) =>
            Callback.successful(request.ok(s"The key $name has value ${value.utf8String}"))
          case None =>
            memcacheClient.get(ByteString(name)).map {
              case Some(Value(_, data, _)) =>
                request.ok(s"The key $name has value ${data.utf8String}")
              case None =>
                request.ok(s"The key $name has no value")
            }
        }

  }
}
// #example
