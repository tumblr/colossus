package colossus.core

import colossus.parsing.DataSize
import colossus.service.ErrorConfig
import colossus.metrics.ConfigHelpers._
import com.typesafe.config.{Config, ConfigException, ConfigFactory}

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.Try

/** Contains values for configuring how a Server operates
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
  * [[colossus.core.server.Initializer]], this determines how long to wait before the
  * initialization is considered a failure and whether to retry the
  * initialization.
  *
  * @param shutdownTimeout Once a Server begins to shutdown, it will signal a
  * request to every open connection.  This determines how long it will wait for
  * every connection to self-terminate before it forcibly closes them and
  * completes the shutdown.
  *   * @param requestTimeout how long to wait until we timeout the request
  * @param requestBufferSize how many concurrent requests a single connection can be processing
  * @param logErrors if true, any uncaught exceptions or service-level errors will be logged
  * @param requestMetrics toggle request metrics
  * @param maxRequestSize max size allowed for requests
  */
case class ServerSettings(
    port: Int,
    slowStart: ConnectionLimiterConfig = ConnectionLimiterConfig.NoLimiting,
    maxConnections: Int,
    maxIdleTime: Duration,
    lowWatermarkPercentage: Double,
    highWatermarkPercentage: Double,
    highWaterMaxIdleTime: FiniteDuration,
    tcpBacklogSize: Option[Int] = None,
    bindingRetry: RetryPolicy,
    delegatorCreationPolicy: WaitPolicy,
    shutdownTimeout: FiniteDuration,
    reuseAddress: Option[Boolean] = None,
    requestTimeout: Duration,
    requestBufferSize: Int,
    logErrors: Boolean,
    requestMetrics: Boolean,
    maxRequestSize: DataSize,
    errorConfig: ErrorConfig
) {
  def lowWatermark: Double  = lowWatermarkPercentage * maxConnections
  def highWatermark: Double = highWatermarkPercentage * maxConnections
}

object ServerSettings {
  val ConfigRoot = "colossus.server"

  lazy val default: ServerSettings = extract(ConfigFactory.load().getConfig(s"$ConfigRoot.default"))

  def extract(config: Config): ServerSettings = {

    val errorConf      = config.getConfig("errors")
    val errorsDoNotLog = errorConf.getStringList("do-not-log").asScala.toSet
    val errorsLogName  = errorConf.getStringList("log-only-name").asScala.toSet
    val errorConfig    = ErrorConfig(errorsDoNotLog, errorsLogName)

    ServerSettings(
      port = config.getInt("port"),
      maxConnections = config.getInt("max-connections"),
      slowStart = ConnectionLimiterConfig.fromConfig(config.getConfig("slow-start")),
      maxIdleTime = config.getScalaDuration("max-idle-time"),
      lowWatermarkPercentage = config.getDouble("low-watermark-percentage"),
      highWatermarkPercentage = config.getDouble("high-watermark-percentage"),
      highWaterMaxIdleTime = config.getFiniteDuration("highwater-max-idle-time"),
      tcpBacklogSize = config.getIntOption("tcp-backlog-size"),
      bindingRetry = RetryPolicy.fromConfig(config.getConfig("binding-retry")),
      delegatorCreationPolicy = WaitPolicy.fromConfig(config.getConfig("delegator-creation-policy")),
      shutdownTimeout = config.getFiniteDuration("shutdown-timeout"),
      reuseAddress = config.getBoolOption("so-reuse-address"),
      requestTimeout = config.getScalaDuration("request-timeout"),
      requestBufferSize = config.getInt("request-buffer-size"),
      logErrors = config.getBoolean("log-errors"),
      requestMetrics = config.getBoolean("request-metrics"),
      maxRequestSize = DataSize(config.getString("max-request-size")),
      errorConfig = errorConfig
    )
  }

  def load(name: String, config: Config = ConfigFactory.load()): ServerSettings = {
    val nameRoot = ConfigRoot + "." + name
    val resolved = try {
      config.getConfig(s"$ConfigRoot.$name").withFallback(config.getConfig(s"$ConfigRoot.default"))
    } catch {
      case ex: ConfigException.Missing =>
        config.getConfig(s"$ConfigRoot.default")
    }

    extract(resolved)

  }
}
