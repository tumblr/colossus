package colossus
package testkit

import core._

import akka.actor._
import akka.testkit.TestProbe

class MockWriteEndpoint(maxBufferSize: Int, workerProbe: TestProbe,handler: Option[ConnectionHandler] = None) 
  extends MockWriteBuffer(maxBufferSize, handler) with WriteEndpoint {

  var disconnectCalled = false
  def id: Long = 9876L

  def disconnect() {
    sendDisconnect(DisconnectCause.Disconnect)
  }

  def disrupt() {
    sendDisconnect(DisconnectCause.Closed)
  }

  protected def sendDisconnect(cause : DisconnectCause) {
    connection_status = ConnectionStatus.NotConnected
    disconnectCalled = true
    handler.foreach{_.connectionTerminated(cause)}
  }

  def status: ConnectionStatus = connection_status

  val worker: ActorRef = workerProbe.ref

  def isWritable = connection_status == ConnectionStatus.Connected && bytesAvailable > 0

  def remoteAddress = None

  def lastTimeDataReceived = 0

  def bytesReceived = 0

  def timeOpen = 0
}
