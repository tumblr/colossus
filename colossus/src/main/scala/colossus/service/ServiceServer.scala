package colossus.service

import colossus.parsing.{DataSize, ParseException}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import akka.event.Logging
import colossus.controller._
import colossus.core.{DisconnectCause, DownstreamEventHandler, UpstreamEventHandler, UpstreamEvents}
import colossus.metrics.collectors.{Counter, Histogram, Rate}
import colossus.metrics.TagMap
import colossus.metrics.logging.ColossusLogging
import colossus.util.ExceptionFormatter._
import colossus.streaming.{BufferedPipe, PullAction, PushResult}
import colossus.util.ConfigCache

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

class ServiceConfigException(err: Throwable) extends Exception("Error loading config", err)

/**
  * Configuration class for a Service Server Connection Handler
  *
  * @param requestTimeout how long to wait until we timeout the request
  * @param requestBufferSize how many concurrent requests a single connection can be processing
  * @param logErrors if true, any uncaught exceptions or service-level errors will be logged
  * @param requestMetrics toggle request metrics
  * @param maxRequestSize max size allowed for requests
  * TODO: remove name from config, this should be the same as a server's name and
  * pulled from the ServerRef, though this requires giving the ServiceServer
  * access to the ServerRef
  */
case class ServiceConfig(requestTimeout: Duration,
                         requestBufferSize: Int,
                         logErrors: Boolean,
                         requestMetrics: Boolean,
                         maxRequestSize: DataSize,
                         errorConfig: ErrorConfig)

object ServiceConfig {

  val CONFIG_ROOT  = "colossus.service"
  val DEFAULT_NAME = "default"

  lazy val Default = this.load(DEFAULT_NAME)

  private val cache = new ConfigCache[ServiceConfig] {

    val baseConfig: Config = ConfigFactory.load()

    def load(name: String): Try[ServiceConfig] = Try {
      val config = try {
        baseConfig
          .getConfig(CONFIG_ROOT + "." + name)
          .withFallback(baseConfig.getConfig(CONFIG_ROOT + "." + DEFAULT_NAME))
      } catch {
        case ex: ConfigException.Missing => baseConfig.getConfig(CONFIG_ROOT + "." + DEFAULT_NAME)
      }
      ServiceConfig.load(config)
    }
  }

  /**
    * Load a ServiceConfig from the loaded config at the path
    * `colossus.service.<name>`.  Any settings not specified at that location
    * will fall back to the defaults located at `colossus.service.default` in the
    * reference.conf file.
    */
  def load(name: String): ServiceConfig = cache.get(name) match {
    case Success(c)   => c
    case Failure(err) => throw new ServiceConfigException(err)
  }

  /**
    * Load a ServiceConfig object from a Config source.  The Config object is
    * expected to be in the form of `colossus.service.default`.  Please refer to
    * the reference.conf file.
    */
  def load(config: Config): ServiceConfig = {
    import colossus.metrics.ConfigHelpers._
    val timeout        = config.getScalaDuration("request-timeout")
    val bufferSize     = config.getInt("request-buffer-size")
    val logErrors      = config.getBoolean("log-errors")
    val requestMetrics = config.getBoolean("request-metrics")
    val maxRequestSize = DataSize(config.getString("max-request-size"))

    val errorConf      = config.getConfig("errors")
    val errorsDoNotLog = errorConf.getStringList("do-not-log").asScala.toSet
    val errorsLogName  = errorConf.getStringList("log-only-name").asScala.toSet
    ServiceConfig(timeout, bufferSize, logErrors, requestMetrics, maxRequestSize, ErrorConfig(errorsDoNotLog, errorsLogName))
  }
}

case class ErrorConfig(doNotLog: Set[String], logOnlyName: Set[String])

object RequestFormatter {
  case class LogMessage(message: String, includeStackTrace: Boolean)
}

trait RequestFormatter[I] {
  import RequestFormatter._

  /**
   * None means do not log.
   * Some(false) means log only the name.
   * Some(true) means log with stack trace (default).
   */
  def logWithStackTrace(error: Throwable): Option[Boolean]

  def format(request: I, error: Throwable): Option[String]

  final def formatLogMessage(request: I, error: Throwable): Option[LogMessage] = {
    logWithStackTrace(error).map { includeStackTrace =>
      val message = format(request, error).getOrElse(request.toString)
      LogMessage(message, includeStackTrace)
    }
  }
}

/**
  * The ErrorConfig here is normally loaded from the ServiceConfig.
  */
class ConfigurableRequestFormatter[I](errorConfig: ErrorConfig) extends RequestFormatter[I] {
  override def logWithStackTrace(error: Throwable): Option[Boolean] = {
    val errorName = error.getClass.getSimpleName
    if (errorConfig.doNotLog.contains(errorName)) {
      None
    } else if (errorConfig.logOnlyName.contains(errorName)) {
      Some(false)
    } else {
      Some(true)
    }
  }

  override def format(request: I, error: Throwable): Option[String] = None
}

class ServiceServerException(message: String) extends Exception(message)

class RequestBufferFullException extends ServiceServerException("Request Buffer full")

//if this exception is ever thrown it indicates a bug
class FatalServiceServerException(message: String) extends ServiceServerException(message)

class DroppedReplyException extends ServiceServerException("Dropped Reply")

trait ServiceUpstream[P <: Protocol] extends UpstreamEvents

/**
  * The ServiceServer provides an interface and basic functionality to create a server that processes
  * requests and returns responses over a codec.
  *
  * A Codec is simply the format in which the data is represented.  Http, Redis protocol, Memcached protocl are all
  * examples(and natively supported).  It is entirely possible to use an additional Codec by creating a Codec to parse
  * the desired protocol.
  *
  * Requests can be processed synchronously or
  * asynchronously.  The server will ensure that all responses are written back
  * in the order that they are received.
  *
  */
class ServiceServer[P <: Protocol](val requestHandler: GenRequestHandler[P])
    extends ControllerDownstream[Encoding.Server[P]]
    with ServiceUpstream[P]
    with UpstreamEventHandler[ControllerUpstream[Encoding.Server[P]]]
    with DownstreamEventHandler[GenRequestHandler[P]]
    with ColossusLogging {
  import ServiceServer._

  type Request  = P#Request
  type Response = P#Response

  def downstream = requestHandler
  downstream.setUpstream(this)

  val incoming = new BufferedPipe[Request](50)

  def config             = requestHandler.config
  implicit val namespace = requestHandler.serverContext.server.namespace
  def name               = requestHandler.serverContext.server.config.name

  val log = Logging(context.worker.system.actorSystem, name.toString())
  val controllerConfig = ControllerConfig(config.requestBufferSize,
                                          metricsEnabled = config.requestMetrics,
                                          inputMaxSize = config.maxRequestSize)

  private val requests = Rate("requests", "connection-handler-requests")
  private val latency  = Histogram("latency", "connection-handler-latency")
  private val errors   = Rate("errors", "connection-handler-errors")
  private val requestsPerConnection =
    Histogram("requests_per_connection", "connection-handler-requests-per-connection")
  private val concurrentRequests = Counter("concurrent_requests", "connection-handler-concurrent-requests")

  //set to true when graceful disconnect has been triggered
  private var disconnecting = false

  //this is set to true when the head of the request queue is ready to write its
  //response but the last time we checked the output buffer it was full
  private var dequeuePaused = false

  private def addError(failure: ProcessingFailure[Request], extraTags: TagMap = TagMap.Empty) {
    val tags = extraTags + ("type" -> failure.reason.metricsName)
    errors.hit(tags = tags)
    if (config.logErrors) {
      requestHandler.requestLogFormat match {
        case Some(formatter) => {
          val logMessage = failure match {
            case RecoverableError(request, reason) =>
              formatter.formatLogMessage(request, reason)
            case IrrecoverableError(reason) =>
              formatter.logWithStackTrace(reason).map(b => RequestFormatter.LogMessage("Invalid Request", b))
          }
          logMessage.foreach { x =>
            if (x.includeStackTrace) {
              error(s"Error processing request: ${x.message}", failure.reason)
            } else {
              error(s"Error processing request: ${x.message}")
            }
          }
        }
        case None => {
          val requestString = failure match {
            case RecoverableError(request, _) => request.toString
            case IrrecoverableError(_) => "Invalid Request"
          }
          error(s"Error processing request: $requestString", failure.reason)
        }
      }
    }
  }

  private case class SyncPromise(request: Request) {
    val creationTime = System.currentTimeMillis

    def isTimedOut(time: Long) =
      !isComplete && config.requestTimeout.isFinite && (time - creationTime) > config.requestTimeout.toMillis

    private var _response: Option[Response] = None
    def isComplete                          = _response.isDefined
    def response =
      if (_response.isDefined) _response.get else throw new Exception("Attempt to use incomplete response")

    def complete(response: Response) {
      _response = Some(response)
      checkBuffer()
    }

  }

  private val requestBuffer    = new java.util.LinkedList[SyncPromise]()
  def currentRequestBufferSize = requestBuffer.size
  private var numRequests      = 0

  override def onIdleCheck(period: FiniteDuration) {
    val time = System.currentTimeMillis
    while (requestBuffer.size > 0 && requestBuffer.peek.isTimedOut(time)) {
      //notice - completing the response will call checkBuffer which will write the error immediately
      requestBuffer.peek.complete(handleFailure(RecoverableError(requestBuffer.peek.request, new TimeoutError)))
    }
  }

  /**
    * Pushes the completed responses down to the controller so they can be returned to the client.
    */
  private def checkBuffer() {
    var continue = true
    while (continue && requestBuffer.size > 0 && requestBuffer.peek.isComplete) {
      val done = requestBuffer.remove()
      upstream.outgoing.push(done.response) match {
        case PushResult.Ok => {
          if (config.requestMetrics) {
            concurrentRequests.decrement()
            val tags = requestHandler.tagDecorator.tagsFor(done.request, done.response)
            requests.hit(tags = tags)
            latency.add(tags = tags, value = (System.currentTimeMillis - done.creationTime).toInt)
          }
        }
        case PushResult.Full(signal) => {
          //this might seem odd to remove the head and then add it back, but
          //it's faster than peeking since we have to do less operations on the
          //linkedlist on average
          requestBuffer.addFirst(done)
          signal.notify { checkBuffer() }
          continue = false
        }
        case other => {
          error(s"invalid state on incoming stream $other")
          upstream.connection.forceDisconnect()
        }
      }
    }
    checkGracefulDisconnect()
  }

  override def onBind() {
    requestHandler.setConnection(upstream.connection)
  }

  override def onConnected() {
    processMessages()
  }

  override def onConnectionTerminated(cause: DisconnectCause) {
    if (config.requestMetrics) {
      requestsPerConnection.add(numRequests)
      concurrentRequests.decrement(amount = requestBuffer.size)
    }
    val exc = new DroppedReplyException
    while (requestBuffer.size > 0) {
      addError(RecoverableError(requestBuffer.remove().request, exc))
    }
  }

  def processMessages() {
    incoming.pullWhile(
      request => {
        numRequests += 1
        val promise = new SyncPromise(request)
        requestBuffer.add(promise)

        /**
          * Notice, if the request buffer is full we're still adding to it, but by skipping
          * processing of requests we can hope to alleviate overloading
          */
        val response: Callback[Response] = if (requestBuffer.size <= config.requestBufferSize) {
          try {
            requestHandler.handleRequest(request)
          } catch {
            case t: ParseException =>
              Callback.successful(handleFailure(IrrecoverableError(t)))
            case t: Throwable => {
              Callback.successful(handleFailure(RecoverableError(request, t)))
            }
          }
        } else {
          Callback.successful(handleFailure(RecoverableError(request, new RequestBufferFullException)))
        }
        if (config.requestMetrics) concurrentRequests.increment()
        response.execute {
          case Success(res) => promise.complete(res)
          case Failure(err) => promise.complete(handleFailure(RecoverableError(promise.request, err)))
        }
        PullAction.PullContinue
      },
      _ => ()
    )
  }

  override def onFatalError(reason: Throwable) =
    FatalErrorAction.Disconnect(Some(handleFailure(IrrecoverableError(reason))))

  private def handleFailure(error: ProcessingFailure[Request]): Response = {
    addError(error)
    requestHandler.handleFailure(error)
  }

  private def checkGracefulDisconnect() {
    if (disconnecting && requestBuffer.size == 0) {
      upstream.shutdown()
    }
  }

  override def shutdown() {
    disconnecting = true
    checkGracefulDisconnect()
  }

}

sealed trait ProcessingFailure[C] {
  def reason: Throwable
}

case class IrrecoverableError[C](reason: Throwable)           extends ProcessingFailure[C]
case class RecoverableError[C](request: C, reason: Throwable) extends ProcessingFailure[C]

object ServiceServer {
  class TimeoutError extends Error("Request Timed out")

}
