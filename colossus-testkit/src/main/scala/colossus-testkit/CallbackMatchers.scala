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
            case (t : Throwable) => println(s"creating error") ; error = Some(t)
          }
        }
        case (Failure(x)) => error = Some(x)
      }
      if(!executed) {
        MatchResult(false, "Callback never executed", "Callback never executed")
      }else{
        val errorMsg = error.fold("")(_.getMessage)
        MatchResult(error.isEmpty, errorMsg, errorMsg)
      }
    }
  }

  def evaluateTo[T](f : T => Any) = new CallbackEvaluateTo[T](f)

}

object CallbackMatchers extends CallbackMatchers
