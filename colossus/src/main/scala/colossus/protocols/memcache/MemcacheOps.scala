package colossus
package protocols.memcache

import akka.util.ByteString
import colossus.protocols.memcache.MemcacheCommand._
import colossus.protocols.memcache.MemcacheReply._
import service._

import scala.language.higherKinds


  /**
   * This trait houses the Memcache API.  It contains implementations for most(not all commands.
   * The only commands not supported yet are:
   * - CAS (upcoming)
   * - admin type commands.
   *
   *
   * Just because these commands are not implemented, doesn't mean they cannot be used.  The implementors of this trait provide
   * a generic 'execute' command, which allows for the execution of arbitrary [[colossus.protocols.memcache.MemcacheCommand]] objects.  The calling code is responsible
   * for handling the raw [[colossus.protocols.memcache.MemcacheReply]].  The only restriction here is that the replies must fall in line
   * with what the [[colossus.protocols.memcache.MemcacheReply]] expects, or the parser will fail to recognize the response as valid.
   *
   *
   * @tparam M
   */
  trait MemcacheClient[M[_]] extends LiftedClient[Memcache, M] {

    import async._

    protected def executeCommand[T](c : MemcacheCommand, key : ByteString)(goodCase : PartialFunction[MemcacheReply, M[T]]) : M[T] = {
      executeAndMap(c) {
        goodCase orElse {
          case x: MemcacheError => failure(MemcacheException.fromMemcacheError(x))
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
      storageAsBooleanCommand(Add(key, value, ttl, flags), key)

    def append(key : ByteString, value : ByteString) : M[Boolean] =
      storageAsBooleanCommand(Append(key, value), key)


    def decr(key : ByteString, value : Long) : M[Option[Long]] =
      counterAsOptionCommand(Decr(key, value), key)


    def delete(key : ByteString) : M[Boolean] = {
      executeCommand(Delete(key), key) {
        case Deleted => success(true)
        case NotFound => success(false)
      }
    }

    def get(key : ByteString) : M[Option[Value]] = {
      executeCommand(Get(key), key){
        case a : Value => success(Some(a))
        case NoData => success(None)
      }
    }

    def getAll(keys : ByteString*) : M[Map[ByteString, Value]] = {
      executeCommand(Get(keys : _*), multiKey(keys)){
        case a : Value => success(Map(a.key->a))
        case Values(x) => success(x.map(y => y.key->y).toMap)
        case NoData => success(Map())
      }
    }

    def incr(key : ByteString, value : Long) : M[Option[Long]] =
      counterAsOptionCommand(Incr(key, value), key)


    def prepend(key : ByteString, value : ByteString) : M[Boolean] =
      storageAsBooleanCommand(Prepend(key, value), key)


    def replace(key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) : M[Boolean] =
      storageAsBooleanCommand(Replace(key, value, ttl, flags), key)


    def set(key : ByteString, value : ByteString, ttl : Int = 0, flags : Int = 0) : M[Boolean] =
      storageAsBooleanCommand(Set(key, value, ttl, flags), key)


    def touch(key : ByteString, ttl : Int = 0) : M[Boolean] = {
      executeCommand(Touch(key, ttl), key){
        case Touched => success(true)
        case NotFound => success(false)
      }
    }
  }
  object MemcacheClient {

  
    implicit object MemcacheClientLifter extends ClientLifter[Memcache, MemcacheClient] {
      
      def lift[M[_]](client: Sender[Memcache,M], clientConfig: Option[ClientConfig])(implicit async: Async[M]) = {
        new BasicLiftedClient(client, clientConfig) with MemcacheClient[M]
      }
    }

  }

  case class UnexpectedMemcacheReplyException(message : String) extends Exception
