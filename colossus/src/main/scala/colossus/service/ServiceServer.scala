package colossus
package service

import core._

import akka.actor._
import akka.event.Logging
import metrics._


import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import Codec._

case class ServiceConfig(
  name: MetricAddress,
  requestTimeout: Duration,
  requestBufferSize: Int = 100 //how many concurrent requests can be processing at once
)

class RequestBufferFullException extends Exception("Request Buffer full")
class DroppedReply extends Error("Dropped Reply")


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
abstract class ServiceServer[I,O](codec: ServerCodec[I,O], config: ServiceConfig, worker: WorkerRef)(implicit ex: ExecutionContext) extends ConnectionHandler  {
  import ServiceServer._
  import WorkerCommand._
  import config._
  import Response._

  implicit val callbackExecutor: CallbackExecutor = CallbackExecutor(worker.worker)
  val log = Logging(worker.system.actorSystem, name.toString())

  val REQUESTS  = name / "requests"
  val LATENCY   = name / "latency"
  val ERRORS    = name / "errors"
  val REQPERCON = name / "requests_per_connection"

  val requests  = worker.metrics.getOrAdd(Rate(REQUESTS, List(1.second, 1.minute)))
  val latency   = worker.metrics.getOrAdd(Histogram(LATENCY, periods = List(1.second, 1.minute), sampleRate = 0.25))
  val errors    = worker.metrics.getOrAdd(Rate(ERRORS, List(1.second, 1.minute)))
  val requestsPerConnection = worker.metrics.getOrAdd(Histogram(REQPERCON, periods = List(1.minute), sampleRate = 0.5, percentiles = List(0.5, 0.75, 0.99)))

  def addError(err: Throwable, extraTags: TagMap = TagMap.Empty) {
    val tags = extraTags + ("type" -> err.getClass.getName.replaceAll("[^\\w]", ""))
    errors.hit(tags = tags)
  }
  
  case class SyncPromise(request: I) {
    val creationTime = System.currentTimeMillis

    def isTimedOut(time: Long) = !isComplete && (time - creationTime) > requestTimeout.toMillis

    private var _response: Option[Completion[O]] = None
    def isComplete = _response.isDefined
    def response = _response.getOrElse(throw new Exception("Attempt to use incomplete response"))

    def complete(response: Completion[O]) {
      _response = Some(response)
      checkBuffer()
    }

  }

  private val requestBuffer = collection.mutable.Queue[SyncPromise]()
  private var writer: Option[WriteEndpoint] = None
  private var partiallyWrittenResponse: Option[SyncPromise] = None
  private var numRequests = 0

  def idleCheck(period: Duration) {
    val time = System.currentTimeMillis
    while (requestBuffer.size > 0 && requestBuffer.head.isTimedOut(time)) {
      //notice - completing the response will call checkBuffer which will write the error immediately
      requestBuffer.head.complete(handleFailure(requestBuffer.head.request, new TimeoutError))
    }
  }
    
  /**
   * This is the only function that actually writes to the channel.  It will
   * write any queued responses until it hits an incomplete promise (or the
   * write buffer fills up)
   */
  private def checkBuffer() {
    writer.map{w => 
      while (requestBuffer.size > 0 && requestBuffer.head.isComplete && w.isWritable) {
        val done = requestBuffer.dequeue()
        val comp = done.response
        requests.hit(tags = comp.tags)
        latency.add(tags = comp.tags, value = (System.currentTimeMillis - done.creationTime).toInt)
        w.write(codec.encode(comp.value)) match {
          case WriteStatus.Partial => {
            partiallyWrittenResponse = Some(done)
          }
          case WriteStatus.Failed => {
            addError(new DroppedReply)
            println("Dropped reply") //todo: make better
          }
          case WriteStatus.Zero => {
            //this should never occur since checking isWritable checks to see
            //if data ia partially buffered.  note that even if we actually
            //write 0 bytes to the channel and the whole response is buffered,
            //we'll still get a Partial writestatus since the whole thing is
            //buffered
            throw new Exception("Attempt to write when data is partially buffered")
          }
          case WriteStatus.Complete => {
            handleOnWrite(comp.onwrite)
          }
        }
      }
    }
  }

  /**
   * this is called if we previously filled up the write buffer and had to wait
   * for it to clear up, and now it's ready
   */
  def readyForData() {
    partiallyWrittenResponse.foreach{r =>
      handleOnWrite(r.response.onwrite)
      partiallyWrittenResponse = None
    }
    checkBuffer()
  }

  def connected(endpoint: WriteEndpoint) {
    writer = Some(endpoint)
    val wid = id.getOrElse(throw new Exception("connected called on unbound service handler"))
  }

  def receivedMessage(message: Any, sender: ActorRef) {}

  def connectionClosed(cause : DisconnectCause) {
    requestsPerConnection.add(numRequests)
    writer = None
  }

  def connectionLost(cause : DisconnectError) {
    connectionClosed(cause)
  }

  def receivedData(data: DataBuffer) {
    codec.decodeAll(data){request =>
      numRequests += 1
      val promise = new SyncPromise(request)
      requestBuffer.enqueue(promise)
      /**
       * Notice, if the request buffer if full we're still adding to it, but by skipping
       * processing of requests we can hope to alleviate overloading
       */
      val response: Response[O] = if (requestBuffer.size < requestBufferSize) {
        try {
          processRequest(request) 
        } catch {
          case t: Throwable => {
            handleFailure(request, t)
          }
        }
      } else {
        handleFailure(request, new RequestBufferFullException)
      }
      val cb: Callback[Completion[O]] = response match {
        case SyncResponse(s) => Callback.successful(s)
        case AsyncResponse(a) => Callback.fromFuture(a)
        case CallbackResponse(c) => c
      }
      cb.execute{
        case Success(res) => promise.complete(res)
        case Failure(err) => promise.complete(handleFailure(promise.request, err))
      }

    }
  }

  private def handleFailure(request: I, reason: Throwable): Completion[O] = {
    addError(reason)
    processFailure(request, reason)
  }

  private def handleOnWrite(w: OnWriteAction) = w match {
    case OnWriteAction.Disconnect => disconnect()
    case _ => {}
  }

  def schedule(in: FiniteDuration, message: Any) {
    writer.foreach{w => 
      worker ! Schedule(in, Message(w.id, message))
    }
  }

  //this is just to make some request processing methods cleaner
  def respond(f: => Response[O]): Response[O] = f

  // ABSTRACT MEMBERS

  protected def processRequest(request: I): Response[O]

  //DO NOT CALL THIS METHOD INTERNALLY, use handleFailure!!
  protected def processFailure(request: I, reason: Throwable): Completion[O]

  // UTIL METHODS

  def disconnect() {
    //TODO: exception on not yet connected?
    writer.foreach{_.disconnect()}
  }
}

object ServiceServer {
  class TimeoutError extends Error("Request Timed out")

}
