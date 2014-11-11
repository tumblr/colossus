package colossus.examples

import colossus.IOSystem
import colossus.core._

/*
 * The BasicSyncHandler is a ConnectionHandler that has has default
 * implementations for most of the methods.  It also stores the WriteEndpoint
 * that is passed in the connected method.
 */
class EchoHandler extends BasicSyncHandler {
  def receivedData(data: DataBuffer){
    endpoint.write(data)
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

