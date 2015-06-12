package colossus
package testkit

import colossus.service.Callback
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.util.{Failure, Success}

trait CallbackMatchers {

  class CallbackEvaluateTo[T](f : T => Any) extends Matcher[Callback[T]] {

    def apply(c: Callback[T]): MatchResult = {
      var executed = false
      var error : Option[Throwable] = None
      c.execute {
        case Success(x) => {
          try{
            executed = true
            f(x)
          }catch {
            case (t : Throwable) => error = Some(t)
          }
        }
        case (Failure(x)) => {
          executed = true
          error = Some(x)
        }
      }
      if(!executed) {
        MatchResult(false, "Callback did not complete", "Callback did not complete")
      }else{
        val errorMsg = error.fold("")(e => s"Callback failed execution with error: ${e}")
        MatchResult(error.isEmpty, errorMsg, errorMsg)
      }
    }
  }

  class CallbackFailTo[T](f : Throwable => Any) extends Matcher[Callback[T]] {

    def apply(c: Callback[T]): MatchResult = {
      var executed = false
      var error : Option[Throwable] = None
      var evaluationError : Option[Throwable] = None
      c.execute {
        case Success(x) => {
          executed = true
        }
        case (Failure(x)) => {
          error = Some(x)
          executed = true
          try {
            f(x)
          }catch {
            case (t : Throwable) => evaluationError = Some(t)
          }
        }
      }

      (executed, error, evaluationError) match {
        case (false, _, _) => MatchResult(false, "Callback did not complete", "Callback did not complete")
        case (true, _, Some(x)) => {
          val errorMsg = s"Callback failure evaluation function threw error $x"
          MatchResult(false, errorMsg, errorMsg)
        }
        case (true, None, None) => {
          val errorMsg = s"Callback evaluated successfully, expected a failure"
          MatchResult(false, errorMsg, errorMsg)
        }
        case (true, Some(x), None) => {
          MatchResult(true, "", "")
        }
      }
    }
  }

  def evaluateTo[T](f : T => Any) = new CallbackEvaluateTo[T](f)

  def failWith[T](f : Throwable => Any) = new CallbackFailTo[T](f)

}

object CallbackMatchers extends CallbackMatchers
