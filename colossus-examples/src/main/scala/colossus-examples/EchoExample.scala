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
class EchoHandler(context: Context) extends BasicSyncHandler(context) with ServerConnectionHandler {
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

