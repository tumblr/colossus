package colossus.examples

import colossus.IOSystem
import colossus.core._

import akka.util.ByteString
import colossus.encoding._

/*
 * The BasicSyncHandler is a ConnectionHandler that has has default
 * implementations for most of the methods.  It also stores the WriteEndpoint
 * that is passed in the connected method.
 */
class EchoHandler extends BasicSyncHandler with ServerConnectionHandler {
  var bytes = ByteString()
  def receivedData(data: DataBuffer){
    bytes = ByteString(data.takeAll)
    endpoint.requestWrite()
  }

  def shutdownRequest(){}

  def readyForData(buffer: DataOutBuffer) = {
    buffer.write(bytes)
    MoreDataResult.Complete
  }
}

class EchoDelegator(server: ServerRef, worker: WorkerRef) extends Delegator(server, worker) {

  def acceptNewConnection = Some(new EchoHandler)
}

object EchoExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {
    val echoConfig = ServerConfig(
      name = "echo",
      settings = ServerSettings(
        port = port
      ),
      delegatorFactory = (server, worker) => new EchoDelegator(server, worker)
    )
    Server(echoConfig)
  
  }

}

