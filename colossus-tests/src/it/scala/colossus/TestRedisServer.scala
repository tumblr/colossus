package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.core.ServerRef
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod.{Get => HttpGet, Post}
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.protocols.redis._
import colossus.service.{ClientConfig, Service}



object TestRedisServer {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {
    import colossus.protocols.redis.Commands._

    import scala.concurrent.duration._

    Service.serve[Http]("test-redis-server", port){ context =>

      val redisClient = new RedisClient(ClientConfig(new InetSocketAddress("localhost", 6379), 1.second, "redis"), context.worker)

      context.handle{ connection =>
        connection.become {
          case req @ Post on Root / "del" / key => {
            redisClient.send(Del(ByteString(key))).map{
              case IntegerReply(x) => req.ok(x.toString)
              case _ => req.error(":(")
            }
          }
          case req @ HttpGet on Root / "exists" / key => {
            redisClient.send(Exists(ByteString(key))).map{
              case IntegerReply(x) => req.ok((x == 1).toString)
              case x => req.error(":(")
            }
          }
          case req @ HttpGet on Root / "get" / key => {
              redisClient.send(Get(ByteString(key))).map{
              case BulkReply(x) => req.ok(x.utf8String)
              case NilReply => req.notFound("")
              case _ => req.error(":(")
            }
          }
          case req @ Post on Root / "set" / key / value => {
              redisClient.send(Set(ByteString(key), ByteString(value))).map{
              case StatusReply(x) => req.ok(x)
              case _ => req.error(":(")
            }
          }
          case req @ Post on Root / "setnx" / key / value => {
            redisClient.send(Setnx(ByteString(key), ByteString(value))).map{
              case IntegerReply(x) => req.ok((x == 1).toString)
              case _ => req.error(":(")
            }
          }
          case req @ Post on Root / "setex" / key / value / time => {
            val seconds = time.toInt.seconds
            redisClient.send(Setex(ByteString(key), ByteString(value), seconds)).map{
              case StatusReply(x) => req.ok(x)
              case _ => req.error(":(")
            }
          }
          case req @ HttpGet on Root / "strlen" / key  => {
            redisClient.send(Strlen(ByteString(key))).map{
              case IntegerReply(x) if x > 0 => req.ok(x.toString)
              case IntegerReply(x) => req.notFound("")
              case _ => req.error(":(")
            }
          }
          case req @ HttpGet on Root / "ttl" / key  => {
            redisClient.send(Ttl(ByteString(key))).map{
              case IntegerReply(-2) => req.notFound()
              case IntegerReply(-1) => req.badRequest("")
              case IntegerReply(x) => req.ok(x.toString)
              case _ => req.error(":(")
            }
          }
        }
      }
    }
  }
}
