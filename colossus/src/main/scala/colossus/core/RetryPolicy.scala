package colossus.core

import colossus.metrics.ConfigHelpers._
import com.typesafe.config.Config
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

object RetryPolicy {

  def fromConfig(obj: Config): RetryPolicy = {
    val policyType = obj.getString("type").toUpperCase
    policyType match {
      case "NONE" => NoRetry
      case "BACKOFF" => {
        val base      = obj.getFiniteDuration("base")
        val mult      = BackoffMultiplier.fromConfig(obj.getConfig("multiplier"))
        val maxTries  = obj.getIntOption("max-tries")
        val maxTime   = obj.getFiniteDurationOption("max-time")
        val immediate = obj.getBoolean("immediate-first-attempt")
        BackoffPolicy(base, mult, maxTime, maxTries, immediate)
      }
      case _ => throw new Exception(s"Unknown RetryPolicy $policyType")
    }
  }

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

  def fromConfig(config: Config) = {
    val mtype = config.getString("type").toUpperCase
    mtype match {
      case "CONSTANT"     => Constant
      case "LINEAR"       => Linear(config.getFiniteDuration("max"))
      case "EXPONENTIAL"  => Exponential(config.getFiniteDuration("max"))
      case _ => throw new Exception(s"Invalid Backoff multiplier $mtype")
    }
  }

  /**
   * A multiplier that will keep the backoff time constant
   */
  case object Constant extends BackoffMultiplier {
    def value(base: FiniteDuration, attempt: Int) = base
  }

  trait IncreasingMultiplier extends BackoffMultiplier {

    def max: FiniteDuration

    //needed to avoid possible out-of-range issue with FiniteDuration
    private var hitMax = false

    def value(base: FiniteDuration, attempt: Int) = {
      if (hitMax) {
        max
      } else {
        val i = increaseValue(base, attempt)
        if (i > max) {
          hitMax = true
          max
        } else {
          i
        }
      }
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
    protected def increaseValue(base: FiniteDuration, attempt: Int) = {
      base * math.pow(2, attempt - 1).toLong
    }
  }
}

/**
 * A [[RetryPolicy]] used to gradually back-off from retries of an operation.
 * This class is flexible enough to handle most common retry schemes, such as
 * constantly retrying for a given amount of time or exponentially backing-off.
 *
 * ===Examples===
 *
 * Retry every 30 seconds for up to 5 minutes
 * {{{
 * BackoffPolicy(30.seconds, BackoffMultiplier.Constant, maxTime = Some(5.minutes), immediateFirstAttempt = false)
 * }}}
 *
 * Immediately retry once, then backoff exponentially starting at 100 milliseconds, doubling with every attempt until the interval in between attempts is 5 seconds
 * {{{
 * BackoffPolicy(100.milliseconds, BackoffMultiplier.Exponential(5.seconds))
 * }}}
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



/**
 * A WaitPolicy describes configuration for any process that needs to wait
 * for some operation to complete, and if/how to retry the operation if it fails
 * to complete within the waiting time.
 *
 * For example, a WaitPolicy is used by [[colossus.core.Server]] to determine
 * how long to wait for it's delegators to start up and how to respond if they
 * fail
 *
 * @param waitTime How long to wait before the operation should be considered timed out
 * @param retryPolicy The policy that dictates how to retry the operation if it either fails or times out
 */
case class WaitPolicy(waitTime : FiniteDuration, retryPolicy : RetryPolicy) {

}


object WaitPolicy {

  def noRetry(waitTime: FiniteDuration): WaitPolicy = WaitPolicy(waitTime, NoRetry)

  def fromConfig(config: Config): WaitPolicy = {
    WaitPolicy(config.getFiniteDuration("wait-time"), RetryPolicy.fromConfig(config.getConfig("retry-policy")))
  }
}
