package colossus.core

import scala.concurrent.duration._

/**
 * A RetryPolicy provides a scheme for managing controlled retries of some
 * operation.  For example, [[colossus.service.ServiceClient]] uses a `RetryPolicy` to determine
 * how it should try to reconnect if it's first attempt to connect fails.
 *
 * RetryPolicy acts as a factory for [[RetryIncident]], which is a per-incident
 * manager of retries.  So everytime a user encounters a sitiation where an
 * operation needs to be retried, it should use the `RetryPolicy` create a new
 * incident, and use the incident until either the operation succeeds or the
 * incident indicates to stop retrying.
 */
trait RetryPolicy {

  def start(): RetryIncident
}

/**
 * A `RetryAttempt` represents the next action that should be taken when retring an operation.
 */
sealed trait RetryAttempt
object RetryAttempt {
  /**
   * The user should stop retrying
   */
  case object Stop extends RetryAttempt

  /**
   * The user should immediately attempt to retry the operation
   */
  case object RetryNow extends RetryAttempt

  /**
   * The user should retry the operation after waiting for the specified period of time
   */
  case class RetryIn(time: FiniteDuration) extends RetryAttempt
}

case class IncidentReport(totalTime: FiniteDuration, totalAttempts: Int)

/**
 * A RetryIncident is a state machine for managing the retry logic of a single
 * incident of a failed operation.  It is generally unaware of what that
 * operation is and takes no action itself, but instead is used by the performer
 * of the operation as a control flow mechanism.  For example, when a
 * [[colossus.service.ServiceClient]] fails to connect to its target host, it
 * will create a new `RetryIncident` from it's given [[RetryPolicy]].  
 *
 * On each successive failure of the operation, a call to [[nextAttempt()]]
 * should be made that will return a `RetryAttempt` indicating what action
 * should be taken.  A single `RetryIncident` cannot be reused should be
 * discarded when either the operation completes or gives up
 *
 */
trait RetryIncident {

  /**
   * Get the next retry attempt, which incidates what action should be taken for
   * the next retry
   */
  def nextAttempt(): RetryAttempt

  /**
   * Returns the total number of retry attempts made
   */
  def attempts: Int

  /**
   * Returns a report indicating the total time taken and total attempts made
   */
  def end() : IncidentReport
}

/**
 * A BackoffMultiplier is used by a [[BackoffPolicy]] determines how the amount
 * of time changes in between retry attempts.  For example, with a [[BackoffMultiplier.Linear]]
 * multiplier (with a base backoff of 50 milliseconds), the amount of time in
 * between retries will increase by 50 milliseconds each time.
 */
trait BackoffMultiplier {
  def value(base: FiniteDuration, attempt: Int): FiniteDuration
}

object BackoffMultiplier {

  /**
   * A multiplier that will keep the backoff time constant
   */
  case object Constant extends BackoffMultiplier {
    def value(base: FiniteDuration, attempt: Int) = base
  }

  trait IncreasingMultiplier extends BackoffMultiplier {
    
    def max: FiniteDuration

    def value(base: FiniteDuration, attempt: Int) = {
      val i = increaseValue(base, attempt)
      if (i > max) max else i
    }

    protected def increaseValue(base: FiniteDuration, attempt: Int): FiniteDuration
  }

  /**
   * A multiplier that will increase the backoff linearly with each attempt, up to `max`
   */
  case class Linear(max: FiniteDuration) extends IncreasingMultiplier {
    protected def increaseValue(base: FiniteDuration, attempt: Int) = base * attempt
  }

  /**
   * A multiplier that will double the backoff time with each attempt, up to `max`
   */
  case class Exponential(max: FiniteDuration) extends IncreasingMultiplier {
    protected def increaseValue(base: FiniteDuration, attempt: Int) = base * math.pow(2, attempt - 1).toLong
  }
}

/**
 * A [[RetryPolicy]] used to gradually back-off from retries of an operation.
 * This class is flexible enough to handle most common retry schemes, such as
 * constantly retrying for a given amount of time or exponentially backing-off.
 *
 * @param baseBackoff The base value to use for backing off.  This may be used by the [[multiplier]]
 * @param multiplier The multiplier that will be applied to the [[baseBackoff]]
 * @param maxTime The maximim amount of time to allow for retrying
 * @param maxTries The maximum number of attempts to make
 * @param immediateFirstAttempt Whether the first retry attempt should be immediate or use the given backoff and multiplier
 */
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
        maxTime.map{t => start + t.toMillis < now}.getOrElse(false) || 
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

/**
 * A [[RetryPolicy]] that will never retry
 */
case object NoRetry extends RetryPolicy with RetryIncident {
  
  def start() = this

  def attempts = 1

  def end() = IncidentReport(0.milliseconds, 1)

  def nextAttempt() = RetryAttempt.Stop

}
