package colossus
package service

import colossus.parsing.DataSize
import colossus.parsing.DataSize._
import core._
import controller._

import akka.event.Logging
import metrics._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import Codec._

/**
 * Configuration class for a Service Server Connection Handler
 * @param requestTimeout how long to wait until we timeout the request
 * @param requestBufferSize how many concurrent requests a single connection can be processing
 * @param logErrors if true, any uncaught exceptions or service-level errors will be logged
 * @param requestMetrics
 * @param maxRequestSize max size allowed for requests
 * TODO: remove name from config, this should be the same as a server's name and
 * pulled from the ServerRef, though this requires giving the ServiceServer
 * access to the ServerRef
 */
case class ServiceConfig(
  requestTimeout: Duration = Duration.Inf,
  requestBufferSize: Int = 100,
  logErrors: Boolean = true,
  requestMetrics: Boolean = true,
  maxRequestSize: DataSize = 1L.MB
)

object ServiceConfig {
  val Default = ServiceConfig()
}

trait RequestFormatter[I] {
  def format(request : I) : String
}

class ServiceServerException(message: String) extends Exception(message)

class RequestBufferFullException extends ServiceServerException("Request Buffer full")

//if this exception is ever thrown it indicates a bug
class FatalServiceServerException(message: String) extends ServiceServerException(message)

class DroppedReplyException extends ServiceServerException("Dropped Reply")


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
abstract class ServiceServer[I,O]
  (codec: ServerCodec[I,O], config: ServiceConfig, serverContext: ServerContext)
extends Controller[I,O](codec,
  ControllerConfig(config.requestBufferSize, Duration.Inf, serverContext.server.config.name, config.maxRequestSize),
  serverContext.context)
with ServerConnectionHandler {
  import ServiceServer._
  import config._
  import context.worker.metrics
  def name = serverContext.server.config.name

  val log = Logging(context.worker.system.actorSystem, name.toString())
  def tagDecorator: TagDecorator[I,O] = TagDecorator.default[I,O]
  def requestLogFormat : Option[RequestFormatter[I]] = None

  
  private val requests  = Rate(name / "requests")
  private val latency   = Histogram(name / "latency", sampleRate = 0.25)
  private val errors    = Rate(name / "errors")
  private val requestsPerConnection = Histogram(name / "requests_per_connection", sampleRate = 0.5, percentiles = List(0.5, 0.75, 0.99))
  private val concurrentRequests = Counter(name / "concurrent_requests")

  //set to true when graceful disconnect has been triggered
  private var disconnecting = false

  //this is set to true when the head of the request queue is ready to write its
  //response but the last time we checked the output buffer it was full
  private var dequeuePaused = false

  private def addError(request: I, err: Throwable, extraTags: TagMap = TagMap.Empty) {
    val tags = extraTags + ("type" -> err.getClass.getName.replaceAll("[^\\w]", ""))
    errors.hit(tags = tags)
    if (logErrors) {
      val formattedRequest = requestLogFormat.map{_.format(request)}.getOrElse(request.toString)
      log.error(err, s"Error processing request: $formattedRequest: $err")
    }
  }

  private case class SyncPromise(request: I) {
    val creationTime = System.currentTimeMillis

    def isTimedOut(time: Long) = !isComplete && requestTimeout.isFinite && (time - creationTime) > requestTimeout.toMillis

    private var _response: Option[O] = None
    def isComplete = _response.isDefined
    def response = _response.getOrElse(throw new Exception("Attempt to use incomplete response"))

    def complete(response: O) {
      _response = Some(response)
      checkBuffer()
    }

  }


  private val requestBuffer = new java.util.LinkedList[SyncPromise]()
  def currentRequestBufferSize = requestBuffer.size
  private var numRequests = 0

  override def idleCheck(period: Duration) {
    super.idleCheck(period)

    val time = System.currentTimeMillis
    while (requestBuffer.size > 0 && requestBuffer.peek.isTimedOut(time)) {
      //notice - completing the response will call checkBuffer which will write the error immediately
      requestBuffer.peek.complete(handleFailure(requestBuffer.peek.request, new TimeoutError))

    }
  }
    
  /**
   * Pushes the completed responses down to the controller so they can be returned to the client.
   */
  private def checkBuffer() {
    while (isConnected && requestBuffer.size > 0 && requestBuffer.peek.isComplete && canPush) {
      val done = requestBuffer.remove()
      val comp = done.response
      if (requestMetrics) concurrentRequests.decrement()
      pushResponse(done.request, comp, done.creationTime) 
    }
    if (!canPush) {
      //this means the output buffer cannot accept any more messages, so we have
      //to pause dequeuing responses and wait for the next message in the output
      //buffer to be written
      dequeuePaused = true
    }
    checkGracefulDisconnect()
  }

  override def connectionClosed(cause : DisconnectCause) {
    super.connectionClosed(cause)
    if (requestMetrics) {
      requestsPerConnection.add(numRequests)
      concurrentRequests.decrement(num = requestBuffer.size)
    }
    val exc = new DroppedReplyException
    while (requestBuffer.size > 0) {
      addError(requestBuffer.remove().request, exc)
    }
  }

  override def connectionLost(cause : DisconnectError) {
    connectionClosed(cause)
  }

  private def pushResponse(request: I, response: O, startTime: Long) {
    if (requestMetrics) {
      val tags = tagDecorator.tagsFor(request, response)
      requests.hit(tags = tags)
      latency.add(tags = tags, value = (System.currentTimeMillis - startTime).toInt)
    }
    val pushed = push(response, startTime) {
      case OutputResult.Success => {
        if (dequeuePaused) {
          dequeuePaused = false
          checkBuffer()
        }
      }
      case f: OutputError => {
        addError(request, f.reason)
      }
    }

    //this should never happen because we are always checking if the outputqueue
    //is full before calling this
    if (!pushed) {
      throw new FatalServiceServerException("Attempted to push response to a full output buffer")
    }
  }

  protected def processMessage(request: I) {
    numRequests += 1
    val promise = new SyncPromise(request)
    requestBuffer.add(promise)
    /**
     * Notice, if the request buffer is full we're still adding to it, but by skipping
     * processing of requests we can hope to alleviate overloading
     */
    val response: Callback[O] = if (requestBuffer.size <= requestBufferSize) {
      try {
        processRequest(request) 
      } catch {
        case t: Throwable => {
          Callback.successful(handleFailure(request, t))
        }
      }
    } else {
      Callback.successful(handleFailure(request, new RequestBufferFullException))
    }
    if (requestMetrics) concurrentRequests.increment()
    response.execute{
      case Success(res) => promise.complete(res)
      case Failure(err) => promise.complete(handleFailure(promise.request, err))
    }
  }

  private def handleFailure(request: I, reason: Throwable): O = {
    addError(request, reason)
    processFailure(request, reason)
  }

  private def checkGracefulDisconnect() {
    if (disconnecting && requestBuffer.size == 0) {
      super.shutdown()
    }
  }

  override def shutdown() {
    pauseReads()
    disconnecting = true
    checkGracefulDisconnect()
  }

  // ABSTRACT MEMBERS

  protected def processRequest(request: I): Callback[O]

  //DO NOT CALL THIS METHOD INTERNALLY, use handleFailure!!
  protected def processFailure(request: I, reason: Throwable): O

}

object ServiceServer {
  class TimeoutError extends Error("Request Timed out")

}
