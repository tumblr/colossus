package colossus.examples

import akka.actor._
import akka.util.ByteString
import colossus.IOSystem
import colossus.core.ServerRef
import colossus.service._
import colossus.protocols.redis._
import colossus.protocols.redis.server._
import scala.concurrent.Promise

class KeyValDB extends Actor {

  import KeyValDB._

  val db = collection.mutable.Map[ByteString, ByteString]()

  def receive = {
    case Get(key, promise) => promise.success(db.get(key))
    case Set(key, value, promise) => {
      db(key) = value
      promise.success(())
    }
  }
}

object KeyValDB {
  case class Get(key: ByteString, promise: Promise[Option[ByteString]] = Promise())
  case class Set(key: ByteString, value: ByteString, promise: Promise[Unit] = Promise())
}

object KeyValExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {

    val db = io.actorSystem.actorOf(Props[KeyValDB])

    RedisServer.start("key-value-example", port) { initContext =>
      new Initializer(initContext) {
        def onConnect = serverContext => new RequestHandler(serverContext) {
          def handle = {
            case Command("GET", args) => {
              val dbCmd = KeyValDB.Get(args(0))
              db ! dbCmd
              Callback.fromFuture(dbCmd.promise.future).map {
                case Some(value) => BulkReply(value)
                case None        => NilReply
              }
            }
            case Command("SET", args) => {
              val dbCmd = KeyValDB.Set(args(0), args(1))
              db ! dbCmd
              Callback.fromFuture(dbCmd.promise.future).map { _ =>
                StatusReply("OK")
              }
            }
          }
        }
      }
    }
  }
}
