package colossus.core

import scala.concurrent.duration._


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
