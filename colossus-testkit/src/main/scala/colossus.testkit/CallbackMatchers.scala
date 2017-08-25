package colossus.testkit

import colossus.service.{CallbackExecutor, Callback}
import org.scalatest.Assertions
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.concurrent.duration.FiniteDuration

trait CallbackMatchers {

  class CallbackEvaluateTo[T](duration: FiniteDuration, f: T => Any)(implicit cbe: CallbackExecutor)
      extends Matcher[Callback[T]] {

    def apply(c: Callback[T]): MatchResult = {
      try {
        val res = CallbackAwait.result(c, duration)
        f(res)
        MatchResult(true, "", "")
      } catch {
        case th: Throwable => MatchResult(false, s"Unexpected exception: $th", s"Unexpected exception: $th")
      }
    }
  }

  class CallbackFailTo[T, Ex <: Throwable: Manifest](duration: FiniteDuration, f: Ex => Any)(
      implicit cbe: CallbackExecutor)
      extends Matcher[Callback[T]]
      with Assertions {

    def apply(c: Callback[T]): MatchResult = {

      val expectedException = implicitly[Manifest[Ex]].runtimeClass.getName

      try {
        CallbackAwait.result(c, duration)
        val msg = s"Expected Callback to fail with $expectedException"
        MatchResult(false, msg, msg)
      } catch {
        case e: Ex => {
          try {
            f(e)
            MatchResult(true, "", "")
          } catch {
            case e: Throwable => {
              val errorMsg = s"Callback evaluation function threw: $e"
              MatchResult(false, errorMsg, errorMsg)
            }
          }
        }
        case e: Throwable => {
          val msg = s"Expected Callback to fail with $expectedException.  Received ${e.getClass.getName} instead"
          MatchResult(false, msg, msg)
        }
      }
    }
  }

  def evaluateTo[T](f: T => Any)(implicit cbe: CallbackExecutor, duration: FiniteDuration): Matcher[Callback[T]] =
    new CallbackEvaluateTo[T](duration, f)

  def failWith[T, U <: Any, Ex <: Throwable: Manifest](f: Ex => U)(implicit cbe: CallbackExecutor,
                                                                   duration: FiniteDuration): Matcher[Callback[T]] =
    new CallbackFailTo[T, Ex](duration, f)

  def failWith[T, Ex <: Throwable: Manifest](implicit cbe: CallbackExecutor,
                                             duration: FiniteDuration): Matcher[Callback[T]] =
    new CallbackFailTo[T, Ex](duration, (ex: Ex) => {})
}

object CallbackMatchers extends CallbackMatchers
