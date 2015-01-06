package colossus
package testkit

import core._

import akka.util.ByteString

import org.scalatest

/**
 * if a handler is passed, the buffer will call the handler's readyForData, and it will call it's own handleWrite if interestRW is true
 */
class MockWriteBuffer(maxWriteSize: Int, handler: Option[ConnectionHandler] = None) extends WriteBuffer {
  var bytesAvailable = maxWriteSize
  var bufferCleared = false
  var writeCalls = collection.mutable.ArrayBuffer[ByteString]()
  var numCallsSinceClear = 0
  var connection_status: ConnectionStatus = ConnectionStatus.Connected

  protected def setKeyInterest(){}

  def onBufferClear(){
    bufferCleared = true
    handler.foreach{_.readyForData()}
  }
  def channelWrite(data: DataBuffer) = {
    if (connection_status != ConnectionStatus.Connected) {
      throw new java.nio.channels.ClosedChannelException
    }
    val written = math.min(data.size, bytesAvailable)
    writeCalls.append(ByteString(data.take(written)))
    numCallsSinceClear += 1
    bytesAvailable -= written
    written
  }
  def clearBuffer() = {
    bytesAvailable = maxWriteSize
    val dataSinceClear = if (numCallsSinceClear > 0) {
      writeCalls.takeRight(numCallsSinceClear).reduce{_ ++ _}
    } else {
      ByteString()
    }
    numCallsSinceClear = 0
    handler.foreach{_ =>
      if (writeReadyEnabled) {
        handleWrite()
      }
    }
    dataSinceClear
  }

  def numWrites = writeCalls.size

    
}
