package colossus.testkit

import akka.util.{ByteString, ByteStringBuilder}
import colossus.core._

/**
  * if a handler is passed, the buffer will call the handler's readyForData, and it will call it's own handleWrite if interestRW is true
  */
trait MockChannelActions extends ChannelActions {

  def maxWriteSize: Int

  protected var bytesAvailable                      = maxWriteSize
  private var writeCalls                            = collection.mutable.Queue[ByteString]()
  protected var connection_status: ConnectionStatus = ConnectionStatus.Connected

  def status = connection_status

  private var bufferCleared = false

  private val writtenData = new ByteStringBuilder

  def channelWrite(data: DataBuffer) = {
    if (bytesAvailable == 0) {
      0
    } else {
      val toTake = math.min(bytesAvailable, data.remaining)
      bytesAvailable -= toTake
      //println(s"$toTake : $bytesAvailable")
      val bytes = ByteString(data.take(toTake))
      writeCalls.enqueue(bytes)
      bytes.size
    }

  }

  def finishConnect() {}

  def keyInterestOps(ops: Int) {}

  def channelClose() {
    connection_status = ConnectionStatus.NotConnected
  }

  def channelHost() = java.net.InetAddress.getLocalHost()

  /*
  def completeDisconnect() {
    connection_status = ConnectionStatus.NotConnected
  }
   */

  //legacy method
  def disconnectCalled = status == ConnectionStatus.NotConnected

  def clearBuffer(): ByteString = {
    println("clearing buffer")
    val lastsize = bytesAvailable
    bytesAvailable = maxWriteSize
    val builder = new ByteStringBuilder
    while (!writeCalls.isEmpty) {
      builder ++= writeCalls.dequeue()
    }
    builder.result
  }

  private def debugDump(debug: Boolean) = if (debug) writeCalls.mkString(",") else ""

  def expectWrite(data: ByteString) {
    assert(writeCalls.size > 0, "expected write, but no write occurred")
    val call = writeCalls.dequeue()
    assert(call == data, s"expected '${data.utf8String}', got '${call.utf8String}'")
  }

  /**
    * Expect exactly `num` writes
    */
  def expectNumWrites(num: Int, debug: Boolean = false) {
    assert(writeCalls.size == num,
           s"""expected exactly $num writes, but ${writeCalls.size} writes occurred ${debugDump(debug)} """)
    writeCalls.clear()
  }

  def expectOneWrite(data: ByteString, debug: Boolean = false) {
    assert(writeCalls.size == 1,
           s"expected exactly one write, but ${writeCalls.size} writes occurred ${debugDump(debug)}")
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

class MockWriteBuffer(val maxWriteSize: Int) extends WriteBuffer with MockChannelActions {
  def completeDisconnect() { channelClose() }

  def testWrite(d: DataBuffer): WriteStatus = write(d)
}
