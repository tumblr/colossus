package colossus
package protocols.redis

import core._
import service._

import akka.actor._
import akka.util.{ByteString, ByteStringBuilder}

import scala.annotation.tailrec

/**
 * This client can be used to handle the redis monitor stream.  It can handle
 * the data from both 2.4 and 2.6+ redis servers.
 *
 * This is not very durable and needs some more work (handling disconnects,
 * some way to handle backpressure)
 */
abstract class RedisMonitorClient extends ClientConnectionHandler {
  
  import RedisMonitorClient._

  def bound(id: Long, worker: WorkerRef){}
  
  private var endpoint: Option[WriteEndpoint] = None


  val codec = new RedisClientCodec

  def receivedData(data: DataBuffer) {
    codec.decode(data).foreach{
      case StatusReply(bytes) if (bytes == ByteString("OK")) => {
        println("beginning stream")
      }
      case StatusReply(data) => {
        val args = parseLine(ByteString(data)) //fix that
        if (args.size > 0 && args(0) != ByteString("MONITOR")) {
          val cmd = args(0).utf8String.toUpperCase
          if (! ignoreCommands.contains(cmd)){
            val command = Command(cmd, args.tail)
            processCommand(command)
          }
        } else {
          println(s"BAD LINE: $data")
        }
      }
      case other => println("ERROR IN STREAM")
    }    
  }

  def receivedMessage(message: Any, sender: ActorRef) {

  }
  def writeAck(status: WriteStatus){}
  def connectionClosed(){} //connection was closed on our end
  def connectionLost(){
    println("CONNECTION LOST")
  }
  def connectionFailed(){
    println("CONNECTION FAILED")
  }
  def readyForData(){}
  def connected(endp: WriteEndpoint) {
    endpoint = Some(endp)
    endp.write(DataBuffer(ByteString("MONITOR\r\n")))
  }

  //callable methods

  def disconnect() {
    endpoint.foreach{_.disconnect()}
  }

  //overridable  stuff 

  val ignoreCommands: List[String] = Nil

  //abstract methods

  def processCommand(cmd: Command)
}

object RedisMonitorClient {
  //be aware, StringContext.escapeSequence does not handle \a
  val validEscapes:Map[Byte, Byte] = Map[Char, Int](
    'a' -> 0x07,
    'b' -> '\b', 
    'f' -> 0x0C,
    'r' -> '\r',
    'n' -> '\n', 
    '\\' -> '\\', 
    't' -> '\t',
    'v' -> 0x0B,
    '"' -> '"'
  ).map{case (k,v) => k.toByte -> v.toByte}

  def parseLine(line: ByteString): List[ByteString] = {
    @tailrec
    def loop(items: List[ByteString], build: ByteStringBuilder, remain: ByteString): List[ByteString] = if (remain.size == 0) {
      items.reverse
    } else remain.head match {
      case '\\' =>  remain.tail.head match {
        case 'x' => {
          val hex = remain.tail.tail.take(2).utf8String
          val value:Int = Integer.parseInt(hex, 16)
          val vbyte = value.toByte
          loop(items, build.putByte(vbyte), remain.tail.drop(3))
        }
        //chars that are escape sequences
        case n if (validEscapes contains n) => loop(items, build.putByte(validEscapes(n)), remain.tail.tail)
        //chars that should be unescaped
        case n => loop(items, build.putByte('\\').putByte(n), remain.tail.tail)
      }
      case '"' => loop(build.result :: items, new ByteStringBuilder, remain.tail.drop(remain.tail.indexOf('"') + 1))
      case other => loop(items, build.putByte(other), remain.tail)      
    }
    loop(Nil, new ByteStringBuilder, line.drop(line.indexOf('"') + 1))
  }
}
