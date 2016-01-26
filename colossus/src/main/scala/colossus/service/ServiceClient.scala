package colossus
package service


import java.net.InetSocketAddress

import akka.actor._
import akka.event.Logging
import colossus.controller._
import colossus.core._
import colossus.metrics._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

import com.github.nscala_time.time.Imports.DateTime


/**
 * Configuration used to specify a Client's parameters
 * @param address The address with which to connect
 * @param requestTimeout The request timeout value
 * @param name The MetricAddress associated with this client
 * @param pendingBufferSize Size of the pending buffer
 * @param sentBufferSize Size of the sent buffer
 * @param failFast  When a failure is detected, immediately fail all pending requests.
 * @param connectionAttempts Polling configuration to govern retry behavior for both initial connect attempts
 *                           and for connection lost events.
 * @param idleTimeout How long the connection can remain idle (both sending and
 *        receiving data) before it is closed.  This should be significantly higher
 *        than requestTimeout.
 *
 */
case class ClientConfig(
  address: InetSocketAddress, 
  requestTimeout: Duration, 
  name: MetricAddress,
  pendingBufferSize: Int = 100,
  sentBufferSize: Int = 100,
  failFast: Boolean = false,
  connectionAttempts : PollingDuration = PollingDuration(500.milliseconds, None),
  idleTimeout: Duration = Duration.Inf
)

class ServiceClientException(message: String) extends Exception(message)

/**
 * Thrown when a request is lost in transit
 * @param message Cause
 */
class ConnectionLostException(message: String) extends ServiceClientException(message)

/**
 * Throw when a request is attempted while not connected
 * @param message Cause
 */
class NotConnectedException(message: String) extends ServiceClientException(message)

/**
 * Thrown when the pending buffer is full
 * @param message Cause
 */
class ClientOverloadedException(message: String) extends ServiceClientException(message)

/**
 * Returned when a request has been pending for too long
 */
class RequestTimeoutException extends ServiceClientException("Request Timed out")

/**
 * Thrown when there's some kind of data error
 * @param message Cause
 */
class DataException(message: String) extends ServiceClientException(message)

trait ServiceClientLike[I,O]  {
  def gracefulDisconnect()
  def send(request: I): Callback[O]
}


/**
 * This is thrown when a Client is manually disconnected, and subsequent attempt is made to reconnect.
 * To simplify the internal workings of Clients, instead of trying to reset its internal state, it throws.  Create
 * a new Client to reestablish a connection.
 * @param msg error message
 */
class StaleClientException(msg : String) extends Exception(msg)


/**
 * A ServiceClient is a non-blocking, synchronous interface that handles
 * sending atomic commands on a connection and parsing their replies
 *
 * Notice - The client will not begin to connect until it is bound to a worker,
 * so when using the default constructor a service client will not connect on
 * it's own.  You must either call `bind` on the client or use the constructor
 * that accepts a worker
 *
 * TODO: make underlying output controller data size configurable
 */
class ServiceClient[I,O](
  codec: Codec[I,O], 
  val config: ClientConfig,
  val worker: WorkerRef
)(implicit tagDecorator: TagDecorator[I,O] = TagDecorator.default[I,O])
extends Controller[O,I](codec, ControllerConfig(config.pendingBufferSize, OutputController.DefaultDataBufferSize , config.requestTimeout)) with ClientConnectionHandler with ServiceClientLike[I,O] with ManualUnbindHandler{

  import colossus.core.WorkerCommand._
  import config._

  type ResponseHandler = Try[O] => Unit

  override val maxIdleTime = config.idleTimeout

  implicit val col = worker.system.metrics.base

  private val requests  = col.getOrAdd( new Rate(name / "requests"))
  private val errors    = col.getOrAdd( new Rate(name / "errors"))
  private val droppedRequests    = col.getOrAdd( new Rate(name / "dropped_requests"))
  private val connectionFailures    = col.getOrAdd( new Rate(name / "connection_failures"))
  private val disconnects  = col.getOrAdd( new Rate(name / "disconnects"))
  private val latency = col.getOrAdd( new Histogram(name / "latency", sampleRate = 0.10, percentiles = List(0.75,0.99)))
  lazy val log = Logging(worker.system.actorSystem, s"client:$address")

  private val responseTimeoutMillis: Long = config.requestTimeout.toMillis

  case class SourcedRequest(message: I, handler: ResponseHandler) {
    val start: Long = System.currentTimeMillis

    def isTimedOut(now: Long) = now > (start + responseTimeoutMillis)
  }


  private var manuallyDisconnected = false
  private var connectionAttempts = 0
  private val sentBuffer    = mutable.Queue[SourcedRequest]()
  private var disconnecting: Boolean = false //set to true during graceful disconnect

  //TODO way too application specific
  private val hpTags: TagMap = Map("client_host" -> address.getHostName, "client_port" -> address.getPort.toString)
  private val hTags: TagMap = Map("client_host" -> address.getHostName)

  worker.bind(this)

  /**
   *
   * @return Underlying WriteEndpoint's ConnectionStatus, defaults to Connecting if there is no WriteEndpoint
   */
  def connectionStatus: ConnectionStatus = if (isConnected) {
    ConnectionStatus.Connected 
  } else if (canReconnect && !manuallyDisconnected) {
    ConnectionStatus.Connecting
  } else {
    ConnectionStatus.NotConnected
  }

  override def onBind(){
    super.onBind()
    if(!manuallyDisconnected){
      log.info(s"client ${id.get} connecting to $address")
      worker ! Connect(address, id.get)
    }else{
      throw new StaleClientException("This client has already been manually disconnected and cannot be reused, create a new one.")
    }
  }

  /**
   * Allow any requests in transit to complete, but cancel all pending requests
   * and don't allow any new ones
   */
  override def gracefulDisconnect() {
    log.info(s"Terminating connection to $address")
    purgePending(new NotConnectedException("Connection is closing"))
    disconnecting = true
    manuallyDisconnected = true
    checkGracefulDisconnect()
  }

  /**
   * Sent a request to the service, along with a handler for processing the response.
   */
  private def sendNow(request: I)(handler: ResponseHandler){
    val s = SourcedRequest(request, handler)
    attemptWrite(s)
  }

  /**
   * Create a callback for sending a request.  this allows you to do something like
   * service.send("request"){response => "YAY"}.map{str => println(str)}.execute()
   */
  def send(request: I): Callback[O] = UnmappedCallback[O](sendNow(request))

  def processMessage(response: O) {
    val now = System.currentTimeMillis
    try {
      val source = sentBuffer.dequeue()
      latency.add(tags = hTags, value = (now - source.start).toInt) //notice only grouping by host for now
      source.handler(Success(response))
      requests.hit(tags = hpTags)
    } catch {
      case e: java.util.NoSuchElementException => {
        throw new DataException(s"No Request for response ${response.toString}!")
      }
    }
    checkGracefulDisconnect()
    if (!writesEnabled) resumeWrites()
  }

  def receivedMessage(message: Any, sender: ActorRef) {}

  override def connected(endpoint: WriteEndpoint) {
    super.connected(endpoint)
    log.info(s"${id.get} Connected to $address")
    connectionAttempts = 0
  }

  private def purgeBuffers(reason : Throwable) {
    sentBuffer.foreach(failRequest(_, reason))
    sentBuffer.clear()
    if (failFast) {
      purgePending(reason)
    }
  }

  override protected def connectionClosed(cause: DisconnectCause): Unit = {
    super.connectionClosed(cause)
    manuallyDisconnected = true
    disconnects.hit(tags = hpTags + ("cause" -> cause.tagString))
    purgeBuffers(new NotConnectedException(s"${cause.logString}"))
  }

  override protected def connectionLost(cause : DisconnectError) {
    super.connectionLost(cause)
    cause match {
      case DisconnectCause.ConnectFailed(error) => {
        log.warning(s"${id.get} failed to connect to ${address.toString}: ${error.getMessage}")
        connectionFailures.hit(tags = hpTags)
        purgeBuffers(new NotConnectedException(s"${cause.logString}"))
      }
      case _ => {
        log.warning(s"${id.get} connection lost to ${address.toString}: ${cause.logString}")
        disconnects.hit(tags = hpTags + ("cause" -> cause.tagString))
        purgeBuffers(new ConnectionLostException(s"${cause.logString}"))
      }
    }
    attemptReconnect()
  }

  override def shutdownRequest() {
    gracefulDisconnect()
  }

  private def attemptReconnect() {
    connectionAttempts += 1
    if(!disconnecting) {
      if(canReconnect) {
        log.warning(s"attempting to reconnect to ${address.toString} after $connectionAttempts unsuccessful attempts.")
        worker ! Schedule(config.connectionAttempts.interval, Connect(address, id.get))
      }
      else {
        log.error(s"failed to connect to ${address.toString} after $connectionAttempts tries, giving up.")
        worker.unbind(id.get)
      }
    } else {
      worker.unbind(id.get)
    }
  }

  private def canReconnect = config.connectionAttempts.isExpended(connectionAttempts)


  private def attemptWrite(s: SourcedRequest) {
    if (disconnecting) {
      // don't allow any new requests, appear as if we're dead
      failRequest(s, new NotConnectedException("Not Connected"))
    } else if (isConnected || !failFast) {
      val pushed = push(s.message, s.start){
        case OutputResult.Success         => {
          sentBuffer.enqueue(s)
          if (sentBuffer.size >= config.sentBufferSize) {
            pauseWrites() //writes resumed in processMessage
          }
        }
        case OutputResult.Failure(err)    => failRequest(s, err)
        case OutputResult.Cancelled(err)  => failRequest(s, err)
      }
      if (!pushed) {
        failRequest(s, new ClientOverloadedException(s"Error sending ${s.message}: Client is overloaded"))
      }
    } else {
      droppedRequests.hit(tags = hpTags)
      failRequest(s, new NotConnectedException("Not Connected"))
    }
  }

  private def checkGracefulDisconnect() {
    if (isConnected && disconnecting && sentBuffer.size == 0) {
      super.gracefulDisconnect()
    }
  }


  override def idleCheck(period: Duration) {
    super.idleCheck(period)

    if (sentBuffer.size > 0 && sentBuffer.front.isTimedOut(System.currentTimeMillis)) {
      // the oldest sent message has expired with no response - kill the connection
      // sending the Kill message instead of disconnecting will trigger the reconnection logic
      worker ! Kill(id.get, DisconnectCause.TimedOut)
    }
  }

  private def failRequest(s: SourcedRequest, exception: Throwable): Unit = {
    // TODO clean up duplicate code https://github.com/tumblr/colossus/issues/274
    errors.hit(tags = hpTags + ("type" -> exception.getClass.getName.replaceAll("[^\\w]", "")))
    s.handler(Failure(exception))
  }
}
