package colossus

import java.net.InetSocketAddress

import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import colossus.core._
import colossus.service.{AsyncServiceClient, ClientConfig}

import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import scala.language.higherKinds

class EchoHandler(c: ServerContext) extends BasicSyncHandler(c.context) with ServerConnectionHandler {

  val buffer = new collection.mutable.Queue[ByteString]
  def receivedData(data: DataBuffer){
    //endpoint.write(data)
    buffer.enqueue(ByteString(data.takeAll))
    endpoint.requestWrite
  }

  override def readyForData(out: DataOutBuffer) = {
    out.write(buffer.dequeue)
    if (buffer.isEmpty) MoreDataResult.Complete else MoreDataResult.Incomplete
  }
      

}

object RawProtocol {
  import colossus.service._

  object RawCodec extends Codec[ByteString, ByteString] {
    def decode(data: DataBuffer) = if (data.hasUnreadData) Some(DecodedResult.Static(ByteString(data.takeAll))) else None
    def encode(raw: ByteString) = DataBuffer(raw)
    def reset(){}
  }

  trait Raw extends CodecDSL {
    type Input = ByteString
    type Output = ByteString
  }
  implicit object ServiceClientLifter extends ServiceClientLifter[Raw, RawClient[Callback]] {

    def lift(_client: CodecClient[Raw])(implicit worker: WorkerRef): RawClient[Callback] = new RawClient[Callback] with CallbackResponseAdapter[Raw] {
      val client = _client
    }
  }

  implicit object FutureClientLifter extends FutureClientLifter[Raw, RawClient[Future]] {
    def lift(_client: FutureClient[Raw])(implicit io: IOSystem): RawClient[Future] = {
      import io.actorSystem.dispatcher
      new RawClient[Future] with FutureResponseAdapter[Raw] {
        val client = _client
        implicit val executionContext: ExecutionContext = implicitly[ExecutionContext]
      }
    }
  }

  object Raw extends ClientFactories[Raw, RawClient]{
    
  }

  trait RawClient[M[_]] extends ResponseAdapter[Raw, M]

  object RawClient {
  }

  

  implicit object RawCodecProvider extends CodecProvider[Raw] {
    def provideCodec() = RawCodec

    def errorResponse(request: ByteString, reason: Throwable) = ByteString(s"Error (${reason.getClass.getName}): ${reason.getMessage}")
  }

  implicit object RawClientCodecProvider extends ClientCodecProvider[Raw] {
    def clientCodec() = RawCodec
    val name = "raw"
  }

}

object TestClient {
  import RawProtocol._

  def apply(io: IOSystem, port: Int, waitForConnected: Boolean = true,
            connectionAttempts : PollingDuration = PollingDuration(250.milliseconds, None)): AsyncServiceClient[ByteString, ByteString] = {
    val config = ClientConfig(
      name = "/test",
      requestTimeout = 100.milliseconds,
      address = new InetSocketAddress("localhost", port),
      pendingBufferSize = 10,
      failFast = true,
      connectionAttempts = connectionAttempts
    )
    val client = AsyncServiceClient[Raw](config)(io, RawClientCodecProvider)
    if (waitForConnected) {
      TestClient.waitForConnected(client)
    }
    client
  }

  def waitForConnected[I,O](client: AsyncServiceClient[I,O], maxTries: Int = 10) {
    waitForStatus(client, ConnectionStatus.Connected, maxTries)
  }

  def waitForStatus[I,O](client: AsyncServiceClient[I, O], status: ConnectionStatus, maxTries: Int = 5) {
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
    while (Await.result((server.server ? Server.GetInfo), 100.milliseconds) != Server.ServerInfo(connections, ServerStatus.Bound)) {
      Thread.sleep(100)
      tries -= 1
      if (tries == 0) {
        throw new Exception(s"Server failed to achieve $connections connections")
      }
    }

  }

}
