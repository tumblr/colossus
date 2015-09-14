package colossus
package protocols

import akka.util.ByteString
import colossus.core.WorkerRef
import colossus.parsing.DataSize
import colossus.protocols.memcache.MemcacheCommand._
import colossus.protocols.memcache.MemcacheReply._
import service._

import scala.language.higherKinds

import scala.concurrent.{ExecutionContext, Future}

package object memcache {

  trait Memcache extends CodecDSL {
    type Input = MemcacheCommand
    type Output = MemcacheReply
  }

  implicit object MemcacheClientProvider extends ClientCodecProvider[Memcache] {
    def clientCodec = new MemcacheClientCodec
    def name = "memcache"
  }

  /**
   * This trait houses the Memcache API.  It contains implementations for most(not all commands.
   * The only commands not supported yet are:
   * - CAS (upcoming)
   * - admin type commands.
   *
   *
   * Just because these commands are not implemented, doesn't mean they cannot be used.  The implementors of this trait provide
   * a generic 'execute' command, which allows for the execution of arbitrary [[colossus.protocols.memcache.MemcacheCommand]] objects.  The calling code is responsible
   * for handling the raw [[colossus.protocols.memcache.MemcacheReply]].
   *
   * @tparam M
   */
  trait MemcacheClient[M[_]] { this : ResponseAdapter[Memcache, M] =>


    protected def executeCommand[T](c : MemcacheCommand, key : ByteString)(goodCase : PartialFunction[MemcacheReply, M[T]]) : M[T] = {
      executeAndMap(c) {
        goodCase orElse {
          case x => failure(UnexpectedMemcacheReplyException(s"Unexpected response $x when executing ${c.commandName.utf8String} on ${key.utf8String}"))
        }
      }
    }

    protected def storageAsBooleanCommand(c : MemcacheCommand, key : ByteString) : M[Boolean] = executeCommand(c, key){
      case Stored => success(true)
      case NotStored => success(false)
    }

    protected def counterAsOptionCommand(c : MemcacheCommand, key : ByteString) : M[Option[Long]] = executeCommand(c, key){
      case Counter(v) => success(Some(v))
      case NotFound => success(None)
    }

    private def multiKey(keys : Seq[ByteString]) = {
      if(keys.isEmpty){
        ByteString("no keys")
      }else if(keys.size == 1){
        keys.head
      }else{
        keys.head ++ ByteString(s" and ${keys.size} other keys")
      }
    }

    def add(key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) : M[Boolean] =
      storageAsBooleanCommand(Add(MemcachedKey(key), value, ttl, flags), key)

    def append(key : ByteString, value : ByteString) : M[Boolean] =
      storageAsBooleanCommand(Append(MemcachedKey(key), value), key)


    def decr(key : ByteString, value : Long) : M[Option[Long]] =
      counterAsOptionCommand(Decr(MemcachedKey(key), value), key)


    def delete(key : ByteString) : M[Boolean] = {
      executeCommand(Delete(MemcachedKey(key)), key) {
        case Deleted => success(true)
        case NotFound => success(false)
      }
    }

    def get(keys : ByteString*) : M[Map[String, Value]] = {
      executeCommand(Get(keys.map(MemcachedKey(_)) : _*), multiKey(keys)){
        case a : Value => success(Map(a.key->a))
        case Values(x) => success(x.map(y => y.key->y).toMap)
      }
    }

    def incr(key : ByteString, value : Long) : M[Option[Long]] =
      counterAsOptionCommand(Incr(MemcachedKey(key), value), key)


    def prepend(key : ByteString, value : ByteString) : M[Boolean] =
      storageAsBooleanCommand(Prepend(MemcachedKey(key), value), key)


    def replace(key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) : M[Boolean] =
      storageAsBooleanCommand(Replace(MemcachedKey(key), value, ttl, flags), key)


    def set(key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) : M[Boolean] =
      storageAsBooleanCommand(Set(MemcachedKey(key), value, ttl, flags), key)


    def touch(key : ByteString, ttl : Int = 0) : M[Boolean] = {
      executeCommand(Touch(MemcachedKey(key), ttl), key){
        case Touched => success(true)
        case NotFound => success(false)
      }
    }
  }

  class MemcacheCallbackClient(val client : ServiceClient[MemcacheCommand, MemcacheReply])
    extends MemcacheClient[Callback] with CallbackResponseAdapter[Memcache]


  class MemcacheFutureClient(val client : AsyncServiceClient[MemcacheCommand, MemcacheReply])
                            (implicit val executionContext : ExecutionContext)
    extends MemcacheClient[Future] with FutureResponseAdapter[Memcache]


  case class UnexpectedMemcacheReplyException(message : String) extends Exception

  object MemcacheClient {

    def callbackClient(config: ClientConfig, worker: WorkerRef, maxSize : DataSize = MemcacheReplyParser.DefaultMaxSize) : MemcacheClient[Callback] with CallbackResponseAdapter[Memcache] = {
      val serviceClient = new ServiceClient[MemcacheCommand, MemcacheReply](new MemcacheClientCodec(maxSize), config, worker)
      new MemcacheCallbackClient(serviceClient)
    }
    def asyncClient(config : ClientConfig, maxSize : DataSize = MemcacheReplyParser.DefaultMaxSize)(implicit io : IOSystem) : MemcacheClient[Future] with FutureResponseAdapter[Memcache] = {
      implicit val ec = io.actorSystem.dispatcher
      val client = AsyncServiceClient(config, new MemcacheClientCodec(maxSize))
      new MemcacheFutureClient(client)
    }
  }
}

