package colossus
package testkit

import core._

import akka.util.{ByteString, ByteStringBuilder}

/**
 * if a handler is passed, the buffer will call the handler's readyForData, and it will call it's own handleWrite if interestRW is true
 */
class MockWriteBuffer(val maxWriteSize: Int, handler: Option[ConnectionHandler] = None) extends WriteBuffer {
  protected var bytesAvailable = maxWriteSize
  private var writeCalls = collection.mutable.Queue[ByteString]()
  protected var connection_status: ConnectionStatus = ConnectionStatus.Connected

  private var bufferCleared = false

  protected def setKeyInterest(){}

  private val writtenData = new ByteStringBuilder

  val internalBufferSize = 1

  def onBufferClear(){
  }

  //this never gets called
  def channelWrite(data: DataBuffer) = 0

  def clearBuffer(): ByteString = {
    val lastsize = bytesAvailable
    bytesAvailable = maxWriteSize
    val builder = new ByteStringBuilder
    while (!writeCalls.isEmpty) {
      builder ++= writeCalls.dequeue()
    }
    if (lastsize == 0) {
      handler.foreach{_.readyForData()}
    }
    builder.result
  }

  override def write(raw: DataBuffer) : WriteStatus = {
    if (bytesAvailable == 0) {
      WriteStatus.Zero
    } else {
      bytesAvailable -= raw.remaining
      val data = ByteString(raw.takeAll)
      writeCalls.enqueue(data)
      if (bytesAvailable <= 0) {
        bytesAvailable = 0
        WriteStatus.Partial
      } else {
        WriteStatus.Complete
      }
    }
    
  }

  def expectWrite(data: ByteString) {
    assert(writeCalls.size > 0, "expected write, but no write occurred")
    val call = writeCalls.dequeue()
    assert(call == data, s"expected '${data.utf8String}', got '${call.utf8String}'")
  }

  /**
   * Expect exactly `num` writes
   */
  def expectNumWrites(num: Int) {
    assert(writeCalls.size == num, s"expected exactly $num writes, but ${writeCalls.size} writes occurred")
    writeCalls.clear()
  }

  def expectOneWrite(data: ByteString) {
    assert(writeCalls.size == 1, s"expected exactly one write, but ${writeCalls.size} writes occurred")
    expectWrite(data)
  }

  def expectNoWrite() {
    assert(writeCalls.size == 0, s"Expected no write, but data '${writeCalls.head.utf8String}' was written")
  }

  def withExpectedWrite(f: ByteString => Unit) {
    assert(writeCalls.size > 0, "expected write, but no write occurred")
    val call = writeCalls.dequeue()
    f(call)
  }
    

    
}
