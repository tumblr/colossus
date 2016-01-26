package colossus
package testkit

import core._

import akka.actor._
import akka.testkit.TestProbe

trait MockConnection extends WriteBuffer with MockChannelActions {self: Connection =>
  
  /**
   * Simulate event-loop iterations, calling readyForData until this buffer
   * fills or everything is written.  This can be used to test backpressure
   * situations
   *
   * Be aware you need to call clearBuffer yourself
   */
  def iterate[T](f: => T): T = {
    val res = f
    while (writeReadyEnabled && handleWrite(new encoding.DynamicBuffer)) {}
    res
  }

  /**
   * Simulates event loop iteration, clearing the buffer on each iteration to avoid any backpressure
   */
  def iterateAndClear() {
    while (writeReadyEnabled) {
      handleWrite(new encoding.DynamicBuffer)
      clearBuffer()
    }
  }
    

  def iterate() = iterate[Unit]({})

  def disrupt() {
    close(DisconnectCause.Closed)
  }

  def testWrite(d: DataBuffer): WriteStatus = write(d)


  def workerProbe: TestProbe
  def serverProbe: Option[TestProbe]


}

object MockConnection {

  def server(handler: ServerConnectionHandler, _maxWriteSize: Int = 1024)(implicit sys: ActorSystem): ServerConnection with MockConnection = {
    val (_workerProbe, worker) = FakeIOSystem.fakeWorkerRef
    val (_serverProbe, server) = FakeIOSystem.fakeServerRef
    handler.setBind(1, worker)
    new ServerConnection(1, handler, server, worker) with MockConnection {
      def maxWriteSize = _maxWriteSize
      def workerProbe = _workerProbe
      def serverProbe = Some(_serverProbe)
    }
  }

  def client(handler: ClientConnectionHandler, fakeworker: FakeWorker, _maxWriteSize: Int)(implicit sys: ActorSystem): ClientConnection with MockConnection = {
    new ClientConnection(1, handler, fakeworker.worker) with MockConnection {
      def maxWriteSize = _maxWriteSize
      def workerProbe = fakeworker.probe
      def serverProbe = None
    }
  }


  def client(handler: ClientConnectionHandler, _maxWriteSize: Int = 1024 )(implicit sys: ActorSystem): ClientConnection with MockConnection = {
    val fakeworker = FakeIOSystem.fakeWorker
    handler.setBind(1, fakeworker.worker)
    client(handler, fakeworker, _maxWriteSize)
  }
}
