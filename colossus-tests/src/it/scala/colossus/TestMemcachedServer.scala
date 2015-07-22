package colossus

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.core.ServerRef
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod.{Get => HttpGet, Post}
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.protocols.memcache.MemcacheReply._
import colossus.protocols.memcache._
import colossus.service.{Callback, ClientConfig, Service}

import net.liftweb.json._
import net.liftweb.json.Serialization.write

object TestMemcachedServer {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {

    import scala.concurrent.duration._
    import MemcacheCommand._

    implicit val formats = DefaultFormats

    Service.serve[Http]("test-memcached-server", port){ context =>

      val memcachedClient = new MemcacheClient(ClientConfig(new InetSocketAddress("localhost", 11211), 1.second, "memcached"), context.worker)

      context.handle{ connection =>
        connection.become {
          case req @ Post on Root / "add" => {
            req.entity.fold(Callback.successful(req.badRequest("body please"))){ x =>
              val params = parse(x.utf8String).extract[WriteParameters]
              memcachedClient.send(Set(MemcachedKey(params.key), ByteString(params.value), params.ttl, params.flags)).map {
                case Stored => req.ok("Stored")
                case NotStored => req.ok("NotStored")
                case _ => req.error(":(")
              }
            }
          }
          case req @ Post on Root / "append" => {
            req.entity.fold(Callback.successful(req.badRequest("body please"))){ x =>
              val params = parse(x.utf8String).extract[WriteParameters]
              memcachedClient.send(Append(MemcachedKey(params.key), ByteString(params.value))).map {
                case Stored => req.ok("Stored")
                case NotStored => req.ok("NotStored")
                case _ => req.error(":(")
              }
            }.recover{case x => println(x); x.printStackTrace(); req.error("shit")}
          }
          case req @ Post on Root / "decr" / key / value => {
            memcachedClient.send(Decr(MemcachedKey(key), value.toLong)).map {
              case Counter(value) => req.ok(value.toString)
              case _ => req.error(":(")
            }
          }
          case req @ Post on Root / "delete" / key => {
            memcachedClient.send(Delete(MemcachedKey(key))).map {
              case Deleted => req.ok("Deleted")
              case NotFound => req.ok("NotFound")
              case _ => req.error(":(")
            }
          }
          case req @ HttpGet on Root / "get" / key => {
            val keys = key.split("_").map(MemcachedKey(_)) //lazy
            memcachedClient.send(Get(keys : _*)).map {
              case a : Value  => req.ok(write(GetReply(a)))
              case a : Values => req.ok(write(GetReplies(a.values.map(x => GetReply(x)))))
              case NoData => req.notFound("")
              case x => req.error(":(")
            }
          }
          case req @ Post on Root / "incr" / key / value => {
            memcachedClient.send(Incr(MemcachedKey(key), value.toLong)).map {
              case Counter(value) => req.ok(value.toString)
              case _ => req.error(":(")
            }
          }
          case req @ Post on Root / "prepend" => {
            req.entity.fold(Callback.successful(req.badRequest("body please"))){ x =>
              val params = parse(x.utf8String).extract[WriteParameters]
              memcachedClient.send(Prepend(MemcachedKey(params.key), ByteString(params.value))).map {
                case Stored => req.ok("Stored")
                case NotStored => req.ok("NotStored")
                case _ => req.error(":(")
              }
            }
          }
          case req @ Post on Root / "replace" => {
            req.entity.fold(Callback.successful(req.badRequest("body please"))){ x =>
              val params = parse(x.utf8String).extract[WriteParameters]
              memcachedClient.send(Replace(MemcachedKey(params.key), ByteString(params.value), params.ttl, params.flags)).map {
                case Stored => req.ok("Stored")
                case NotStored => req.ok("NotStored")
                case _ => req.error(":(")
              }
            }
          }
          case req @ Post on Root / "set" => {
            req.entity.fold(Callback.successful(req.badRequest("body please"))){ x =>
              val params = parse(x.utf8String).extract[WriteParameters]
              memcachedClient.send(Set(MemcachedKey(params.key), ByteString(params.value), params.ttl, params.flags)).map {
                case Stored => req.ok("Stored")
                case NotStored => req.ok("NotStored")
                case _ => req.error(":(")
              }
            }
          }
          case req @ Post on Root / "touch" / key / time => {

            val seconds = time.toInt
            memcachedClient.send(Touch(MemcachedKey(key), seconds)).map {
              case Touched=> req.ok("Touched")
              case NotFound => req.notFound()
              case _ => req.error(":(")
            }
          }
        }
      }
    }
  }
  case class WriteParameters(key : String, value : String, flags : Int = 0, ttl : Int = 0)

  case class GetReply(key : String, value : String, flags : Int)

  case class GetReplies(values : Seq[GetReply])

  object GetReply {
    def apply(value : Value) : GetReply = GetReply(value.key, value.data.utf8String, value.flags)
  }

}
