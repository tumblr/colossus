package colossus
package core

import com.typesafe.config.Config
import scala.concurrent.duration._

/**
 * Used to control slow start of servers, this will exponentially increase its
 * limit over a period of time until max is reached.  Servers use this to gradually increase the
 * number of allowable open connections during the first few seconds of startup,
 * which can help alleviate thundering herd problems due to JVM warmup.
 *
 * Initializing a limiter with `initial` equal to `max` will essentially disable any slow ramp
 */
class ConnectionLimiter(val initial: Int, val max: Int, val duration: FiniteDuration) {

  private var currentLimit = initial
  private var startTime = 0L
  private var complete: Boolean = initial == max

  private var lastCheck: Long = 0

  private val numIncrements: Int = math.max(1, ((math.log(max - initial) / math.log(2)) + 0.5).toInt)
  private val checkFreq = (duration / numIncrements).toMillis

  def currentTime: Long = System.currentTimeMillis

  def begin() {
    startTime = currentTime
  }

  def limit = if (complete) {
    max
  } else {
    currentLimit = math.min(max, math.pow(2, (currentTime - startTime) / checkFreq + 1).toInt)
    if (currentLimit == max) {
      complete = true
    }
    currentLimit
  }
}

object ConnectionLimiter {

  def noLimiting(max: Int) = new ConnectionLimiter(max, max, 1.second)

  def apply(maxConnections: Int, config: ConnectionLimiterConfig) : ConnectionLimiter = {
    if (config.enabled) {
      new ConnectionLimiter(config.initial, maxConnections, config.duration)
    } else {
      noLimiting(maxConnections)
    }
  }

  def fromConfig(maxConnections: Int, config: Config) = apply(maxConnections, ConnectionLimiterConfig.fromConfig(config))
}

/**
 * Configuration object for connection limiting, used as part of server
 * settings.  This does not contain the max value because that is already part
 * of [[ServerSettings]]
 */
case class ConnectionLimiterConfig(enabled: Boolean, initial: Int, duration: FiniteDuration)

object ConnectionLimiterConfig {
  import colossus.metrics.ConfigHelpers._

  def fromConfig(config: Config): ConnectionLimiterConfig = {
    val enabled   = config.getBoolean("enabled")
    val initial   = config.getInt("initial")
    val duration  = config.getFiniteDuration("duration")
    ConnectionLimiterConfig(enabled, initial, duration)
  }

  /**
   * Default config to use when not using slow-start.  Notice that since enabled
   * is set to false, the values of initial and duration are not used
   */
  val NoLimiting = ConnectionLimiterConfig(false, 0, 1.second)

}
