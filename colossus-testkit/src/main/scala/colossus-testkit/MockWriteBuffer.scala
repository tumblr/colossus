package colossus
package testkit

import core._

import akka.util.{ByteString, ByteStringBuilder}

/**
 * if a handler is passed, the buffer will call the handler's readyForData, and it will call it's own handleWrite if interestRW is true
 */
class MockWriteBuffer(val maxWriteSize: Int, handler: Option[ConnectionHandler] = None) extends WriteBuffer {
  var bytesAvailable = maxWriteSize
  private var writeCalls = collection.mutable.Queue[ByteString]()
  var connection_status: ConnectionStatus = ConnectionStatus.Connected

  private var bufferCleared = false

  protected def setKeyInterest(){}

  private val writtenData = new ByteStringBuilder

  def onBufferClear(){
    bufferCleared = true
    handler.foreach{_.readyForData()}
  }
  def channelWrite(data: DataBuffer) = {
    if (connection_status != ConnectionStatus.Connected) {
      throw new java.nio.channels.ClosedChannelException
    }
    val written = math.min(data.size, bytesAvailable)
    val to_write = ByteString(data.take(written))
    writeCalls.enqueue(to_write)
    writtenData.append(to_write)

    bytesAvailable -= written
    written
  }

  def clearBuffer() = {
    bytesAvailable = maxWriteSize
    if (writeReadyEnabled) {
      handleWrite()
    }
    val written = writtenData.result
    writtenData.clear()
    written
  }

  def expectWrite(data: ByteString) {
    assert(writeCalls.size > 0, "expected write, but no write occurred")
    val call = writeCalls.dequeue()
    assert(call == data, s"expected '${data.utf8String}', got '${call.utf8String}'")
  }

  def expectOneWrite(data: ByteString) {
    assert(writeCalls.size == 1, s"expected exactly one write, but ${writeCalls.size} writes occurred")
    expectWrite(data)
  }

  def expectNoWrite() {
    assert(writeCalls.size == 0, s"Expected no write, but data '${writeCalls.head.utf8String}' was written")
  }

  def expectBufferCleared() {
    assert(bufferCleared == true, "Expected write buffer to be cleared, but it was no")
    bufferCleared = false
  }

  def expectBufferNotCleared() {
    assert(bufferCleared == false, "Expected write buffer to not be cleared, but it was")
  }

    
}
