package colossus
package service


import java.net.InetSocketAddress

import akka.actor._
import akka.event.Logging
import parsing.DataSize
import parsing.DataSize._
import controller._
import core._
import metrics._
import util.ExceptionFormatter._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

/**
 * Configuration used to specify a Client's parameters
 * @param address The address with which to connect
 * @param requestTimeout The request timeout value
 * @param name The MetricAddress associated with this client
 * @param pendingBufferSize Size of the pending buffer
 * @param sentBufferSize Size of the sent buffer
 * @param failFast  When a failure is detected, immediately fail all pending requests.
 * @param connectRetry Retry policy for connections.
 * @param idleTimeout How long the connection can remain idle (both sending and
 *        receiving data) before it is closed.  This should be significantly higher
 *        than requestTimeout.
 * @param maxResponseSize max allowed response size -- larger responses are dropped
 */
case class ClientConfig(
  address: InetSocketAddress,
  requestTimeout: Duration,
  name: MetricAddress,
  pendingBufferSize: Int = 100,
  sentBufferSize: Int = 100,
  failFast: Boolean = false,
  connectRetry : RetryPolicy = BackoffPolicy(50.milliseconds, BackoffMultiplier.Exponential(5.seconds)),
  idleTimeout: Duration = Duration.Inf,
  maxResponseSize: DataSize = 1.MB
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

/**
 * This is thrown when a Client is manually disconnected, and subsequent attempt is made to reconnect.
 * To simplify the internal workings of Clients, instead of trying to reset its internal state, it throws.  Create
 * a new Client to reestablish a connection.
 * @param msg error message
 */
class StaleClientException(msg : String) extends Exception(msg)


sealed trait ClientState
object ClientState {
  

  case object Initializing  extends ClientState 
  case object Connecting    extends ClientState 
  case object Connected     extends ClientState
  case object ShuttingDown  extends ClientState
  case object Terminated    extends ClientState

}
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
class ServiceClient[P <: Protocol](
  codec: Codec[P#Input,P#Output],
  val config: ClientConfig,
  context: Context
)(implicit tagDecorator: TagDecorator[P#Input,P#Output] = TagDecorator.default[P#Input,P#Output])
extends Controller[P#Output,P#Input](codec, ControllerConfig(config.pendingBufferSize, config.requestTimeout, config.maxResponseSize), context)
with ClientConnectionHandler with Sender[P, Callback] with ManualUnbindHandler {

  type I = P#Input
  type O = P#Output

  def this(codec: Codec[P#Input,P#Output], config: ClientConfig, worker: WorkerRef) {
    this(codec, config, worker.generateContext())
  }

  context.worker.worker ! WorkerCommand.Bind(this)

  import colossus.core.WorkerCommand._
  import config._
  implicit val namespace = context.worker.system.namespace / config.name

  type ResponseHandler = Try[O] => Unit

  override val maxIdleTime = config.idleTimeout

  private val requests            = Rate("requests")
  private val errors              = Rate("errors")
  private val droppedRequests     = Rate("dropped_requests")
  private val connectionFailures  = Rate("connection_failures")
  private val disconnects         = Rate("disconnects")
  private val latency             = Histogram("latency",       sampleRate = 0.10, percentiles = List(0.75,0.99))
  private val transitTime         = Histogram("transit_time",  sampleRate = 0.02, percentiles = List(0.5))
  private val queueTime           = Histogram("queue_time",    sampleRate = 0.02, percentiles = List(0.5))

  lazy val log = Logging(worker.system.actorSystem, s"client:$address")

  private val responseTimeoutMillis: Long = config.requestTimeout.toMillis

  case class SourcedRequest(message: I, handler: ResponseHandler, queueTime: Long, sendTime: Long) {
    def isTimedOut(now: Long) = now > (queueTime + responseTimeoutMillis)
  }

  private var clientState: ClientState = ClientState.Initializing

  private var retryIncident: Option[RetryIncident] = None
  private val sentBuffer    = mutable.Queue[SourcedRequest]()

  //TODO way too application specific
  private val hpTags: TagMap = Map("client_host" -> address.getHostName, "client_port" -> address.getPort.toString)
  private val hTags: TagMap = Map("client_host" -> address.getHostName)

  def connectionStatus: ConnectionStatus = clientState match {
    case ClientState.Connected => ConnectionStatus.Connected
    case ClientState.Initializing | ClientState.Connecting => ConnectionStatus.Connecting
    case _ => ConnectionStatus.NotConnected
  }

  /**
   * returns true if the client is potentially able to send.  This does not
   * necessarily mean any new requests will actually be sent, but rather that
   * the client will make an attempt to send it.  This returns false when the
   * client either failed to connect (including retries) or when it has been
   * shut down.
   */
  def canSend = clientState match {
    case ClientState.Connected => true
    case ClientState.Initializing | ClientState.Connecting => !failFast
    case _ => false
  }

  override def onBind(){
    super.onBind()
    if(clientState == ClientState.Initializing){
      log.info(s"client ${id} connecting to $address")
      worker ! Connect(address, id)
      clientState = ClientState.Connecting
      
    }else{
      throw new StaleClientException("This client has already been manually disconnected and cannot be reused, create a new one.")
    }
  }

  /**
   * Sent a request to the service, along with a handler for processing the response.
   */
  private def sendNow(request: I)(handler: ResponseHandler){
    if (canSend) {
      val queueTime = System.currentTimeMillis
      val pushed = push(request, queueTime){
        case OutputResult.Success         => {
          val s = SourcedRequest(request, handler, queueTime, System.currentTimeMillis)
          sentBuffer.enqueue(s)
          this.queueTime.add((s.sendTime - s.queueTime).toInt, hpTags)
          if (sentBuffer.size >= config.sentBufferSize) {
            pauseWrites() //writes resumed in processMessage
          }
        }
        case OutputResult.Failure(err)    => failRequest(handler, err)
        case OutputResult.Cancelled(err)  => failRequest(handler, err)
      }
      if (!pushed) {
        failRequest(handler, new ClientOverloadedException(s"Error sending ${request}: Client is overloaded"))
      }
    } else {
      droppedRequests.hit(tags = hpTags)
      failRequest(handler, new NotConnectedException("Not Connected"))
    }
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
      latency.add(tags = hpTags, value = (now - source.queueTime).toInt)
      transitTime.add(tags = hpTags, value = (now - source.sendTime).toInt)
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
    log.info(s"${id} Connected to $address")
    clientState = ClientState.Connected
    retryIncident = None
  }

  private def purgeBuffers(reason : Throwable) {
    sentBuffer.foreach { s => failRequest(s.handler, reason) } 
    sentBuffer.clear()
    if (failFast) {
      purgePending(reason)
    }
  }

  override protected def connectionClosed(cause: DisconnectCause): Unit = {
    super.connectionClosed(cause)
    clientState = ClientState.Terminated
    disconnects.hit(tags = hpTags + ("cause" -> cause.tagString))
    purgeBuffers(new NotConnectedException(s"${cause.logString}"))
  }

  override protected def connectionLost(cause : DisconnectError) {
    super.connectionLost(cause)
    cause match {
      case DisconnectCause.ConnectFailed(error) => {
        log.warning(s"${id} failed to connect to ${address.toString}: ${error.getMessage}")
        connectionFailures.hit(tags = hpTags)
        purgeBuffers(new NotConnectedException(s"${cause.logString}"))
      }
      case _ => {
        log.warning(s"${id} connection lost to ${address.toString}: ${cause.logString}")
        disconnects.hit(tags = hpTags + ("cause" -> cause.tagString))
        purgeBuffers(new ConnectionLostException(s"${cause.logString}"))
      }
    }
    attemptReconnect()
  }


  private def attemptReconnect() {
    if(clientState != ClientState.ShuttingDown) {
      val incident = retryIncident.getOrElse{
        val i = connectRetry.start()
        retryIncident = Some(i)
        i
      }
      incident.nextAttempt match {
        case RetryAttempt.Stop => {
          val report = incident.end()
          retryIncident = None
          clientState = ClientState.Terminated
          log.error(s"failed to connect to ${address.toString} (${report.totalAttempts} attempts over ${report.totalTime}), giving up.")
          worker.unbind(id)
        }
        case RetryAttempt.RetryNow => {
          log.warning(s"attempting to reconnect to ${address.toString} after ${incident.attempts} unsuccessful attempts.")
          clientState = ClientState.Connecting
          worker ! Connect(address, id)
        }
        case RetryAttempt.RetryIn(time) => {
          log.warning(s"attempting to reconnect to ${address.toString} in ${time} after ${incident.attempts} unsuccessful attempts.")
          clientState = ClientState.Connecting
          worker ! Schedule(time, Connect(address, id))
        }
      }
    } else {
      worker.unbind(id)
    }
  }

  override def shutdown() {
    log.info(s"Terminating connection to $address")
    clientState = ClientState.ShuttingDown
    checkGracefulDisconnect()
  }

  private def checkGracefulDisconnect() {
    if (clientState == ClientState.ShuttingDown && sentBuffer.size == 0) {
      super.shutdown()
    }
  }


  override def idleCheck(period: Duration) {
    super.idleCheck(period)

    if (sentBuffer.size > 0 && sentBuffer.front.isTimedOut(System.currentTimeMillis)) {
      // the oldest sent message has expired with no response - kill the connection
      // sending the Kill message instead of disconnecting will trigger the reconnection logic
      worker ! Kill(id, DisconnectCause.TimedOut)
    }
  }

  private def failRequest(handler: ResponseHandler, exception: Throwable): Unit = {
    // TODO clean up duplicate code https://github.com/tumblr/colossus/issues/274
    errors.hit(tags = hpTags + ("type" -> exception.metricsName))
    handler(Failure(exception))
  }
}

object ServiceClient {

  def apply[C <: Protocol] = ClientFactory.serviceClientFactory[C]

}

