package colossus

import java.net.{Socket, SocketException}

import akka.util.ByteString
import colossus.core._


class EchoHandler extends BasicSyncHandler {
  def receivedData(data: DataBuffer){ 
    endpoint.write(data)
  }
}

class TestConnection(port: Int) {
  val socket = new Socket("localhost", port)
  val input = socket.getInputStream
  val output = socket.getOutputStream

  def write(data: ByteString) {
    try {
      output.write(data.toArray)
      output.flush()
    } catch {
      case s: SocketException => {}
    }
  }

  def read(): ByteString = {
    val buffer = new Array[Byte](1024)
    val num = input.read(buffer)
    ByteString.fromArray(buffer, 0, num)
  }

  def close() {
    socket.close()
  }

  //the only reliable way to check if the connection is closed is to try
  //writing and then read
  def isClosed = {
    try {
      write(ByteString("wat"))
      Thread.sleep(50)
      read()
      false
    }catch {
      case e : SocketException => true
      case _ : Throwable => false
    }
  }

}

