package colossus.examples

import colossus.IOSystem
import colossus.core._

import akka.util.ByteString

/*
 * The BasicSyncHandler is a ConnectionHandler that has has default
 * implementations for most of the methods.  It also stores the WriteEndpoint
 * that is passed in the connected method.
 */
class EchoHandler(context: ServerContext) extends BasicSyncHandler(context.context) with ServerConnectionHandler {
  var bytes = ByteString()
  def receivedData(data: DataBuffer){
    bytes = ByteString(data.takeAll)
    endpoint.requestWrite()
  }

  override def readyForData(buffer: DataOutBuffer) = {
    buffer.write(bytes)
    MoreDataResult.Complete
  }
}

object EchoExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {

    Server.basic("echo", port)(new EchoHandler(_))

  }

}

