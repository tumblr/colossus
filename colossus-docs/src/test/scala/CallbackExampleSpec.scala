import akka.actor.ActorSystem
import akka.testkit.TestKit
import colossus.service.{Callback, CallbackExecutor}
import colossus.testkit.{CallbackAwait, FakeIOSystem}
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.util.Try
import scala.concurrent.duration._

class CallbackExampleSpec extends TestKit(ActorSystem()) with WordSpecLike with BeforeAndAfterAll {

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "My callback" must {

    "return the meaning of life" in {
      // #example
      // callback executor is where the callback runs and also required an actor system to be in scope
      implicit val callbackExecutor: CallbackExecutor = FakeIOSystem.testExecutor

      val callbackUnderTest: Callback[Int] = Callback.complete(Try(42))

      val result = CallbackAwait.result(callbackUnderTest, 1.second)

      assert(result == 42)
      // #example
    }
  }
}
