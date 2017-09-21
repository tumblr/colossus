package colossus.streaming

import colossus.core._
import colossus.controller._
import colossus.testkit._

import org.scalamock.scalatest.MockFactory
class StreamServiceSpec extends ColossusSpec with MockFactory with ControllerMocks {

  trait FooEncoding extends Encoding {
    type Input  = Int
    type Output = Int
  }

  "StreamTranscodingController" must {
    "reset both pipes when connection is terminated" in {
      val upstream = stub[ControllerUpstream[FooEncoding]]
      val up       = new BufferedPipe[Int](10)
      (upstream.outgoing _).when().returns(up)

      val downstream = stub[ControllerDownstream[FooEncoding]]
      val down       = new BufferedPipe[Int](10)
      (downstream.incoming _).when().returns(down)

      val transcoder = new Transcoder[FooEncoding, FooEncoding] {
        def transcodeInput(source: colossus.streaming.Source[Int]): colossus.streaming.Source[Int] = source.map {
          _ * 10
        }
        def transcodeOutput(source: colossus.streaming.Source[Int]): colossus.streaming.Source[Int] = source.map {
          _ * 10
        }
      }

      val controller = new StreamTranscodingController(downstream, transcoder) {
        def onFatalError(reason: Throwable) = FatalErrorAction.Terminate
      }
      controller.setUpstream(upstream)
      controller.connected()
      controller.incoming.push(3) mustBe PushResult.Ok
      controller.incoming.push(4) mustBe PushResult.Ok
      down.pull() mustBe PullResult.Item(30)

      controller.connectionTerminated(DisconnectCause.Disconnect)

      controller.incoming.push(5) mustBe a[PushResult.Full]
      controller.incoming.pull() mustBe a[PullResult.Empty]

    }
  }
}
