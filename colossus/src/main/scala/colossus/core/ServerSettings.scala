package colossus.core

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._

/** Contains values for configuring how a Server operates
  *
  * These are all lower-level configuration settings that are for the most part
  * not concerned with a server's application behavior
  *
  * The low/high watermark percentages are used to help mitigate connection
  * overload.  When a server hits the high watermark percentage of live
  * connections, it will change the idle timeout from `maxIdleTime` to
  * `highWaterMaxIdleTime` in an attempt to more aggressively close idle
  * connections.  This will continue until the percentage drops below
  * `lowWatermarkPercentage`.  This can be totally disabled by just setting both
  * watermarks to 1.
  *
  * @param port Port on which this Server will accept connections
  * @param maxConnections Max number of simultaneous live connections
  * @param lowWatermarkPercentage Percentage of live/max connections which represent a normal state
  * @param highWatermarkPercentage Percentage of live/max connections which represent a high water state.
  * @param maxIdleTime Maximum idle time for connections when in non high water conditions
  * @param highWaterMaxIdleTime Max idle time for connections when in high water conditions.
  * @param tcpBacklogSize Set the max number of simultaneous connections awaiting accepting, or None for NIO default
  *
  * @param bindingRetry A [[colossus.core.RetryPolicy]] describing how to retry
  * binding to the port if the first attempt fails.  By default it will keep
  * retrying forever.
  *
  * @param delegatorCreationPolicy A [[colossus.core.WaitPolicy]] describing how
  * to handle delegator startup.  Since a Server waits for a signal from the
  * [[colossus.IOSystem]] that every worker has properly initialized a
  * [[colossus.core.Initializer]], this determines how long to wait before the
  * initialization is considered a failure and whether to retry the
  * initialization.
  *
  * @param shutdownTimeout Once a Server begins to shutdown, it will signal a
  * request to every open connection.  This determines how long it will wait for
  * every connection to self-terminate before it forcibly closes them and
  * completes the shutdown.
  */
case class ServerSettings(
    port: Int,
    slowStart: ConnectionLimiterConfig = ConnectionLimiterConfig.NoLimiting,
    maxConnections: Int = 1000,
    maxIdleTime: Duration = Duration.Inf,
    lowWatermarkPercentage: Double = 0.75,
    highWatermarkPercentage: Double = 0.85,
    highWaterMaxIdleTime: FiniteDuration = 100.milliseconds,
    tcpBacklogSize: Option[Int] = None,
    bindingRetry: RetryPolicy = BackoffPolicy(100.milliseconds, BackoffMultiplier.Exponential(1.second)),
    delegatorCreationPolicy: WaitPolicy =
      WaitPolicy(500.milliseconds, BackoffPolicy(50.milliseconds, BackoffMultiplier.Constant)),
    shutdownTimeout: FiniteDuration = 100.milliseconds,
    reuseAddress: Option[Boolean] = None
) {
  def lowWatermark  = lowWatermarkPercentage * maxConnections
  def highWatermark = highWatermarkPercentage * maxConnections
}

object ServerSettings {
  val ConfigRoot = "colossus.server"

  def extract(config: Config): ServerSettings = {
    import colossus.metrics.ConfigHelpers._

    val bindingRetry            = RetryPolicy.fromConfig(config.getConfig("binding-retry"))
    val delegatorCreationPolicy = WaitPolicy.fromConfig(config.getConfig("delegator-creation-policy"))

    ServerSettings(
      port = config.getInt("port"),
      maxConnections = config.getInt("max-connections"),
      slowStart = ConnectionLimiterConfig.fromConfig(config.getConfig("slow-start")),
      maxIdleTime = config.getScalaDuration("max-idle-time"),
      lowWatermarkPercentage = config.getDouble("low-watermark-percentage"),
      highWatermarkPercentage = config.getDouble("high-watermark-percentage"),
      highWaterMaxIdleTime = config.getFiniteDuration("highwater-max-idle-time"),
      tcpBacklogSize = config.getIntOption("tcp-backlog-size"),
      bindingRetry = bindingRetry,
      delegatorCreationPolicy = delegatorCreationPolicy,
      shutdownTimeout = config.getFiniteDuration("shutdown-timeout"),
      reuseAddress = config.getBoolOption("so-reuse-address")
    )
  }

  def load(name: String, config: Config = ConfigFactory.load()): ServerSettings = {
    val nameRoot = ConfigRoot + "." + name
    val resolved = if (config.hasPath(nameRoot)) {
      config.getConfig(nameRoot).withFallback(config.getConfig(ConfigRoot))
    } else {
      config.getConfig(ConfigRoot)
    }
    extract(resolved)

  }
}
