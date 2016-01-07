package colossus
package testkit

import core._

import akka.actor._
import akka.testkit.TestProbe

class MockWriteEndpoint(maxBufferSize: Int, workerProbe: TestProbe,handler: Option[ConnectionHandler] = None) 
  extends MockWriteBuffer(maxBufferSize) with WriteEndpoint {

  var disconnectCalled = false
  var disconnectCompleted = false
  def id: Long = 9876L

  def disconnect() {
    disconnectCalled = true
    gracefulDisconnect()
  }

  override def completeDisconnect() {
    super.completeDisconnect()
    sendDisconnect(DisconnectCause.Disconnect)
  }

  def disrupt() {
    sendDisconnect(DisconnectCause.Closed)
  }

  protected def sendDisconnect(cause : DisconnectCause) {
    disconnectCompleted = true
    handler.foreach{_.connectionTerminated(cause)}
  }

  def status: ConnectionStatus = connection_status

  val worker: ActorRef = workerProbe.ref

  def isWritable = connection_status == ConnectionStatus.Connected && bytesAvailable > 0

  def remoteAddress = None

  def lastTimeDataReceived = 0

  def bytesReceived = 0

  def timeOpen = 0

  /**
   * Simulate event-loop iterations, calling readyForData until this buffer
   * fills or everything is written.  This can be used to test backpressure
   * situations
   *
   * Be aware you need to call clearBuffer yourself
   */
  def iterate[T](f: => T): T = {
    val res = f
    handler.foreach{handler =>
      while (writeReadyEnabled && handleWrite(new encoding.DynamicBuffer, handler)) {}
    }
    res
  }

  /**
   * Simulates event loop iteration, clearing the buffer on each iteration to avoid any backpressure
   */
  def iterateAndClear() {
    handler.foreach{handler =>
      while (writeReadyEnabled && handleWrite(new encoding.DynamicBuffer, handler)) {
        clearBuffer()
      }
    }
  }
    

  def iterate() = iterate[Unit]({})

}
