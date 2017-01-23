package colossus

import java.net.InetSocketAddress

import akka.util.{ByteString, Timeout}
import colossus.core._
import controller.{Codec, Encoding}
import colossus.service.{FutureClient, ClientConfig, Protocol}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.higherKinds

class NoopHandler(context: Context) extends CoreHandler(context) with ServerConnectionHandler with ClientConnectionHandler {
  def this(s: ServerContext) = this(s.context)

  def receivedData(data: DataBuffer){}
  def readyForData(out: DataOutBuffer): MoreDataResult = MoreDataResult.Complete

   protected def connectionClosed(cause: colossus.core.DisconnectCause): Unit = {}
   protected def connectionLost(cause: colossus.core.DisconnectError): Unit = {}
   def idleCheck(period: scala.concurrent.duration.Duration): Unit = {}

}

class EchoHandler(c: ServerContext) extends NoopHandler(c){

  val buffer = new collection.mutable.Queue[ByteString]
  override def receivedData(data: DataBuffer){
    //endpoint.write(data)
    buffer.enqueue(ByteString(data.takeAll))
    connectionState match {
      case a: AliveState => a.endpoint.requestWrite()
      case _ => {}
    }
  }

  override def readyForData(out: DataOutBuffer) = {
    out.write(buffer.dequeue)
    if (buffer.isEmpty) MoreDataResult.Complete else MoreDataResult.Incomplete
  }
  
  def connectionFailed(){}
      

}

object SimpleProtocol {
  //this differs from the raw protocol in that messages have a length, so we can encode multiple messages into a single bytestring

  trait SimpleEncoding extends Encoding {
    type Input = ByteString
    type Output = ByteString
  }

  class SimpleCodec extends Codec[SimpleEncoding] {
    import parsing.Combinators._
    def newparser = bytes(intUntil(';') >> {_.toInt}) >> {bytes => ByteString(bytes)}
    var parser = newparser

    def decode(data: DataBuffer) = parser.parse(data)
    def encode(bytes: ByteString, buffer: DataOutBuffer) { 
      buffer write ByteString(bytes.length.toString)
      buffer write ';'
      buffer write bytes
    }

    def reset() {
      parser = newparser
    }
    def endOfStream() = None
  }

  trait Simple extends Protocol {
    type Request = ByteString
    type Response = ByteString
  }
}

object RawProtocol {
  import colossus.service._

  trait BaseRawCodec  {
    def decode(data: DataBuffer) = if (data.hasUnreadData) Some(ByteString(data.takeAll)) else None
    def encode(raw: ByteString, buffer: DataOutBuffer) { buffer write raw }
    def reset(){}
    def endOfStream() = None
  }

  object RawServerCodec extends BaseRawCodec with Codec.Server[Raw]
  object RawClientCodec extends BaseRawCodec with Codec.Client[Raw]

  trait Raw extends Protocol {
    type Request = ByteString
    type Response = ByteString
  }

  implicit object RawClientLifter extends ClientLifter[Raw, RawClient] {
    
    def lift[M[_]](client: Sender[Raw,M], clientConfig: Option[ClientConfig])(implicit async: Async[M]) = {
      new BasicLiftedClient(client, clientConfig) with RawClient[M]
    }
  }

  object Raw extends ClientFactories[Raw, RawClient]{
    implicit def clientFactory = ServiceClientFactory.basic("raw", () => RawClientCodec)
    
  }

  trait RawClient[M[_]] extends LiftedClient[Raw, M]

  object RawClient {
  }

  val RawServer = server.Server

  object server extends BasicServiceDSL[Raw] {

    def provideCodec() = RawServerCodec

    def errorMessage(error: ProcessingFailure[ByteString]) = ByteString(s"Error (${error.reason.getClass.getName}): ${error.reason.getMessage}")


  }

}

object TestClient {
  import RawProtocol._

  def apply(
    io: IOSystem,
    port: Int,
    waitForConnected: Boolean = true,
    connectRetry : RetryPolicy = BackoffPolicy(50.milliseconds, BackoffMultiplier.Exponential(5.seconds))
  ) : FutureClient[Raw] = {
    val config = ClientConfig(
      name = "/test",
      requestTimeout = 100.milliseconds,
      address = new InetSocketAddress("localhost", port),
      pendingBufferSize = 10,
      failFast = true,
      connectRetry = connectRetry
    )
    val client = FutureClient[Raw](config)(io, Raw.clientFactory)
    if (waitForConnected) {
      TestClient.waitForConnected(client)
    }
    client
  }

  def waitForConnected[P <: Protocol](client: FutureClient[P], maxTries: Int = 10) {
    waitForStatus(client, ConnectionStatus.Connected, maxTries)
  }

  def waitForStatus[P <: Protocol](client: FutureClient[P], status: ConnectionStatus, maxTries: Int = 5) {
    var tries = maxTries
    var last = Await.result(client.connectionStatus, 10.seconds)
    while (last != status) {
      Thread.sleep(100)
      tries -= 1
      if (tries == 0) {
        throw new Exception(s"Test client failed to achieve status $status, last status was $last")
      }
      last = Await.result(client.connectionStatus, 10.seconds)
    }
  }

}


object TestUtil {
  def expectServerConnections(server: ServerRef, connections: Int, maxTries: Int = 10) {
    var tries = maxTries
    implicit val timeout = Timeout(100.milliseconds)
    while (Await.result(server.info(), 100.milliseconds) != Server.ServerInfo(connections, ServerStatus.Bound)) {
      Thread.sleep(100)
      tries -= 1
      if (tries == 0) {
        throw new Exception(s"Server failed to achieve $connections connections")
      }
    }

  }

}
