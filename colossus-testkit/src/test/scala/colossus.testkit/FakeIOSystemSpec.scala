package colossus.testkit

import colossus.service.Callback

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

class FakeIOSystemSpec extends ColossusSpec with CallbackMatchers {

  "TestExecutor" must {
    "execute" in {
      implicit val ex = FakeIOSystem.testExecutor

      val cb = Callback.fromFuture(Future { 5 }).map { i =>
        i + 1
      }

      CallbackAwait.result(cb, 1.second) must equal(6)
    }
  }

  "fakeExecutorWorkerRef" must {
    "execute a callback" in {
      val worker = FakeIOSystem.fakeExecutorWorkerRef
      import worker.callbackExecutor
      val cb = Callback.fromFuture(Future { 5 }).map { i =>
        i + 1
      }
      CallbackAwait.result(cb, 1.second) must equal(6)
    }
  }

}
