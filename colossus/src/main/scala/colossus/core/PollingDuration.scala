package colossus.core

import scala.concurrent.duration._

trait RetryPolicy {

  def start(): RetryIncident
}

sealed trait RetryAttempt
object RetryAttempt {
  case object Stop extends RetryAttempt
  case object RetryNow extends RetryAttempt
  case class RetryIn(time: FiniteDuration) extends RetryAttempt
}

case class IncidentReport(totalTime: FiniteDuration, totalAttempts: Int)

trait RetryIncident {
  def nextAttempt(): RetryAttempt

  def attempts: Int
  def end() : IncidentReport
}

trait BackoffMultiplier {
  def value(base: FiniteDuration, attempt: Int): FiniteDuration
}

object BackoffMultiplier {
  case object Constant extends BackoffMultiplier {
    def value(base: FiniteDuration, attempt: Int) = base
  }

  trait IncreasingMultiplier extends BackoffMultiplier {
    
    def max: FiniteDuration

    def value(base: FiniteDuration, attempt: Int) = {
      val i = increaseValue(base, attempt)
      if (i > max) max else i
    }

    def increaseValue(base: FiniteDuration, attempt: Int): FiniteDuration
  }

  case class Linear(max: FiniteDuration) extends IncreasingMultiplier {
    def increaseValue(base: FiniteDuration, attempt: Int) = base * attempt
  }

  case class Exponential(max: FiniteDuration) extends IncreasingMultiplier {
    def increaseValue(base: FiniteDuration, attempt: Int) = base * math.pow(2, attempt - 1).toLong
  }
}

case class BackoffPolicy(
  baseBackoff: FiniteDuration,
  multiplier: BackoffMultiplier,
  maxTime: Option[FiniteDuration] = None,
  maxTries: Option[Int] = None,
  immediateFirstAttempt: Boolean = true
) extends RetryPolicy {

  class BackoffIncident extends RetryIncident {
    private var attempt = 0
    def attempts = attempt
    private val start = System.currentTimeMillis
    import RetryAttempt._

    def nextAttempt() = {
      val now = System.currentTimeMillis
      if (
        maxTime.map{t => start + t.toMillis > now}.getOrElse(false) || 
        maxTries.map{_ <= attempt}.getOrElse(false)
      ) {
        Stop
      } else {
        attempt += 1
        if (immediateFirstAttempt && attempt == 1) {
          RetryNow
        } else {
          RetryIn(multiplier.value(baseBackoff, attempt))
        }
      }
    }

    def end() = IncidentReport((System.currentTimeMillis - start).milliseconds, attempt)
  }

  def start() = new BackoffIncident
}

case object NoRetry extends RetryPolicy with RetryIncident {
  
  def start() = this

  def attempts = 1

  def end() = IncidentReport(0.milliseconds, 1)

  def nextAttempt() = RetryAttempt.Stop

}

//TODO : Fully replace this with RetryPolicy
      

/**
 * Simple class which contains parameters for configuring a polling operation
 * @param interval The interval of the poll
 * @param maximumTries The number of times to execute the poll
 */
case class PollingDuration(interval : FiniteDuration, maximumTries : Option[Long]) {

  def isExpended(tries : Long) : Boolean = {
    maximumTries.fold(true)(_ > tries)
  }

}


object PollingDuration {
  /**
   * Adds support for specifying maximumTries in terms of a [[scala.concurrent.duration.FiniteDuration]]
   * @param interval The interval of the poll
   * @param maxDuration The maximum amount of time to execute the poll.  None means indefinitely.
   * @return
   */
  def apply(interval : FiniteDuration, maxDuration : FiniteDuration) : PollingDuration = {
    PollingDuration(interval, Some(Math.round(maxDuration / interval)))
  }

  val NoRetry = PollingDuration(100.milliseconds, Some(0L))
}
