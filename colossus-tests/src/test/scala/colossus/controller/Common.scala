package colossus.controller

import akka.actor._
import akka.util.ByteString
import colossus.RawProtocol.{Raw, RawServerCodec}
import colossus.core.{ConnectionManager, CoreDownstream, CoreUpstream, DynamicOutBuffer}
import org.scalamock.scalatest.MockFactory
import colossus.parsing.DataSize._
import colossus.streaming.{BufferedPipe, Pipe}
import colossus.testkit.FakeIOSystem

trait ControllerMocks extends MockFactory { self: org.scalamock.scalatest.MockFactory with org.scalatest.Suite =>

  val defaultConfig = ControllerConfig(4, 2000.bytes)

  class TestDownstream[E <: Encoding](config: ControllerConfig)(implicit actorsystem: ActorSystem)
      extends ControllerDownstream[E] {

    val pipe = new BufferedPipe[E#Input](3)

    def incoming = pipe

    def controllerConfig = config

    def context = FakeIOSystem.fakeContext

    def namespace = colossus.metrics.MetricSystem.deadSystem

    override def onFatalError(reason: Throwable) = {
      println(s"FATAL : $reason")
      FatalErrorAction.Terminate
    }
  }

  class TestUpstream[E <: Encoding](val outgoing: Pipe[E#Output, E#Output] = new BufferedPipe[E#Output](2))
      extends ControllerUpstream[E] {
    val connection = stub[ConnectionManager]
    (connection.isConnected _).when().returns(true)

    val pipe = outgoing

  }

  def get(config: ControllerConfig = defaultConfig)(implicit sys: ActorSystem)
    : (CoreUpstream, Controller[Encoding.Server[Raw]], TestDownstream[Encoding.Server[Raw]]) = {
    get(RawServerCodec, config)
  }

  def get[E <: Encoding](codec: Codec[E], config: ControllerConfig)(
      implicit sys: ActorSystem): (CoreUpstream, Controller[E], TestDownstream[E]) = {
    val upstream   = stub[CoreUpstream]
    val downstream = new TestDownstream[E](config)
    val controller = new Controller(downstream, codec)
    controller.setUpstream(upstream)
    (upstream, controller, downstream)
  }

  def expectWrite(c: CoreDownstream, expected: ByteString, bufferSize: Int = 100) {
    val d = new DynamicOutBuffer(bufferSize)
    c.readyForData(d)
    assert(ByteString(d.data.takeAll) == expected)
  }
}
