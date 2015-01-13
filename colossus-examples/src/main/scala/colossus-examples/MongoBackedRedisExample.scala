package colossus.examples

import java.net.InetSocketAddress

import akka.util.ByteString
import colossus.IOSystem
import colossus.core.ServerRef
import colossus.protocols.mongodb.Mongo
import colossus.protocols.mongodb.command.FindAndModify
import colossus.protocols.mongodb.message.{Message, QueryMessage, Reply}
import colossus.protocols.redis._
import colossus.service._
import colossus.util.bson.BsonDsl._
import colossus.util.bson.Implicits._

object MongoBackedRedisExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {

    Service.serve[Redis]("mongo-backed-redis-example", port) { context =>
      val client = new LoadBalancingClient[Message, Reply](
        context.worker,
        sa => context.clientFor[Mongo](sa.getHostName(), sa.getPort()),
        initialClients = Seq(new InetSocketAddress("localhost", 27017)))

      context.handle { connection =>
        connection.become {
          case Command("GET", args) => {
            if (args.length != 1) {
              Callback.successful(ErrorReply("ERR wrong number of arguments for 'get' command"))
            } else {
              val key = args(0).utf8String
              val command = QueryMessage("colossus.redis", document("_id" := key))

              client.send(command).map { reply =>
                reply.documents.toList match {
                  case doc :: Nil => doc.getAs[String]("value") match {
                    case None => NilReply
                    case Some(value) => BulkReply(ByteString(value))
                  }
                  case _ => NilReply
                }
              }
            }
          }

          case Command("SET", args) => {
            if (args.length != 2) {
              Callback.successful(ErrorReply("ERR wrong number of arguments for 'set' command"))
            } else {
              val key = args(0).utf8String
              val value = args(1).utf8String
              val command = FindAndModify(
                "colossus",
                "redis",
                query = Some(document("_id" := key)),
                removeOrUpdate = Right(document("value" := value)),
                upsert = true)

              client.send(command).map(_ => StatusReply("OK"))
            }
          }
        }
      }
    }
  }
}
        
