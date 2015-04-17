
package colossus
package testkit

import akka.testkit.TestProbe
import service._
import scala.concurrent.{Future, ExecutionContext}
import ExecutionContext.Implicits.global


class FakeIOSystemSpec extends ColossusSpec with CallbackMatchers {

  "FakeExecutor" must {
    "execute" in {
      implicit val ex = FakeIOSystem.callingThreadCallbackExecutor
      val cb = Callback.fromFuture(Future.successful(5)).map{i => i + 1}
      cb must evaluateTo{x: Int => x must equal(6)}
    }
  }

}
