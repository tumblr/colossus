package colossus
package service


import java.net.InetSocketAddress

import akka.event.Logging
import com.typesafe.config.{ConfigFactory, Config}
import colossus.parsing.DataSize
import parsing.DataSize._
import controller._
import core._
import metrics._
import util.ExceptionFormatter._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import streaming._

/**
 * Configuration used to specify a Client's parameters
  *
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
  pendingBufferSize: Int = 500,
  sentBufferSize: Int = 100,
  failFast: Boolean = false,
  connectRetry : RetryPolicy = BackoffPolicy(50.milliseconds, BackoffMultiplier.Exponential(5.seconds)),
  idleTimeout: Duration = Duration.Inf,
  maxResponseSize: DataSize = 1.MB
)

object ClientConfig {

  /**
    * Load a ClientConfig definition from a Config.  Looks into `colossus.clients.$clientName` and falls back onto
    * `colossus.client-defaults`
    * @param clientName The name of the client definition to load
    * @param config A config object which contains at the least a `colossus.clients.$clientName` and a `colossus.client-defaults`
    * @return
    */
  def load(clientName : String, config : Config = ConfigFactory.load()) : ClientConfig = {
    val defaults = config.getConfig("colossus.client.defaults")
    load(config.getConfig(s"colossus.client.$clientName").withFallback(defaults))
  }

  /**
    * Create a ClientConfig from a config source.
    *
    * @param config A Config object in the shape of `colossus.client-defaults`.  It is also expected to have the `address` and `name` fields.
    * @return
    */
  def load(config : Config) : ClientConfig = {
    import colossus.metrics.ConfigHelpers._
    val address = config.getInetSocketAddress("address")
    val name = config.getString("name")
    val requestTimeout = config.getScalaDuration("request-timeout")
    val pendingBufferSize = config.getInt("pending-buffer-size")
    val sentBufferSize = config.getInt("sent-buffer-size")
    val failFast = config.getBoolean("fail-fast")
    val connectRetryPolicy = RetryPolicy.fromConfig(config.getConfig("connect-retry-policy"))
    val idleTimeout = config.getScalaDuration("idle-timeout")
    val maxResponseSize = DataSize(config.getString("max-response-size"))
    ClientConfig(address, requestTimeout, name, pendingBufferSize, sentBufferSize, failFast, connectRetryPolicy, idleTimeout, maxResponseSize)
  }
}

class ServiceClientException(message: String) extends Exception(message)

/**
 * Thrown when a request is lost in transit
  *
  * @param message Cause
 */
class ConnectionLostException(message: String) extends ServiceClientException(message)

/**
 * Throw when a request is attempted while not connected
  *
  * @param message Cause
 */
class NotConnectedException(message: String) extends ServiceClientException(message)

/**
 * Thrown when the pending buffer is full
  *
  * @param message Cause
 */
class ClientOverloadedException(message: String) extends ServiceClientException(message)

/**
 * Returned when a request has been pending for too long
 */
class RequestTimeoutException extends ServiceClientException("Request Timed out")

/**
 * Thrown when there's some kind of data error
  *
  * @param message Cause
 */
class DataException(message: String) extends ServiceClientException(message)

/**
 * This is thrown when a Client is manually disconnected, and subsequent attempt is made to reconnect.
 * To simplify the internal workings of Clients, instead of trying to reset its internal state, it throws.  Create
 * a new Client to reestablish a connection.
  *
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


//this is needed because trying to mix in any trait to CoreHandler when constructing causes the compiler to crash.

class UnbindHandler(ds: CoreDownstream, tail: HandlerTail) extends PipelineHandler(ds, tail) with ManualUnbindHandler

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
  val config: ClientConfig,
  val context: Context
)(implicit tagDecorator: TagDecorator[P] = TagDecorator.default[P])
extends ControllerDownstream[Encoding.Client[P]] with HasUpstream[ControllerUpstream[Encoding.Client[P]]] with Client[P, Callback] with HandlerTail {

  type Request = Encoding.Client[P]#Output
  type Response = Encoding.Client[P]#Input

  private val responseTimeoutMillis: Long = config.requestTimeout.toMillis

  class SourcedRequest(val message: Request, handler: ResponseHandler) {
    private var handled = false
    val queueTime = System.currentTimeMillis
    private var _sendTime = 0L
    def sendTime = _sendTime

    def markSent() {
      _sendTime = System.currentTimeMillis
    }

    def complete(result: Try[Response]) {
      if (!handled) {
        handled = true
        handler(result)
      }
    }
    


    def isTimedOut(now: Long) = now > (queueTime + responseTimeoutMillis)
  }


  val controllerConfig = ControllerConfig(config.pendingBufferSize, metricsEnabled = true, inputMaxSize = config.maxResponseSize)

  import colossus.core.WorkerCommand._
  import config._
  implicit val namespace = context.worker.system.namespace / config.name
  private def worker = context.worker
  private def connection = upstream.connection
  def id = context.id

  def connectionState = connection.connectionState

  type ResponseHandler = Try[Response] => Unit

  //override val maxIdleTime = config.idleTimeout

  private val requests            = Rate("requests", "client-requests")
  private val errors              = Rate("errors", "client-errors")
  private val droppedRequests     = Rate("dropped_requests", "client-dropped-requests")
  private val connectionFailures  = Rate("connection_failures", "client-connection-failures")
  private val disconnects         = Rate("disconnects", "client-disconnects")
  private val latency             = Histogram("latency", "client-latency")
  private val transitTime         = Histogram("transit_time", "client-transit-time")
  private val queueTime           = Histogram("queue_time", "client-queue-time")

  lazy val log = Logging(worker.system.actorSystem, s"client:$address")

  private var clientState: ClientState = ClientState.Initializing

  val incoming = new BufferedPipe[Response](config.sentBufferSize)

  private val pending = new BufferedPipe[SourcedRequest](config.pendingBufferSize)
  private val sentBuffer    = new BufferedPipe[SourcedRequest](config.sentBufferSize)


  private var retryIncident: Option[RetryIncident] = None

  //TODO way too application specific
  private val hpTags: TagMap = Map("client_host" -> address.getHostName, "client_port" -> address.getPort.toString)

  def connectionStatus: Callback[ConnectionStatus] = Callback.successful(clientState match {
    case ClientState.Connected => ConnectionStatus.Connected
    case ClientState.Initializing | ClientState.Connecting => ConnectionStatus.Connecting
    case _ => ConnectionStatus.NotConnected
  })

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
    if(clientState == ClientState.Initializing){
      //use the sent buffer as a valve, which will control the number of in-flight messages
      pending into Sink.valve(upstream.outgoing.mapIn[SourcedRequest]{sourced => sourced.markSent;sourced.message}, sentBuffer)
      log.info(s"client $id connecting to $address")
      worker ! Connect(address, id)
      clientState = ClientState.Connecting

    }else{
      throw new StaleClientException("This client has already been manually disconnected and cannot be reused, create a new one.")
    }
  }

  /**
   * Sent a request to the service, along with a handler for processing the response.
   */
  private def sendNow(request: Request)(handler: ResponseHandler){
    val sourced = new SourcedRequest(request, handler)
    if (canSend) {
      pending.push(sourced) match {
        case PushResult.Ok => {}
        case PushResult.Error(reason) => failRequest(sourced, reason)
        case PushResult.Full(trig) => failRequest(sourced, new ClientOverloadedException(s"Error sending ${request}: Client is overloaded"))
        case PushResult.Closed => failRequest(sourced, new Exception("WAT"))
      }
    } else {
      droppedRequests.hit(tags = hpTags)
      failRequest(sourced, new NotConnectedException("Not Connected"))
    }
  }

  /**
   * Create a callback for sending a request.  this allows you to do something like
   * service.send("request"){response => "YAY"}.map{str => println(str)}.execute()
   */
  def send(request: Request): Callback[P#Response] = UnmappedCallback[P#Response](sendNow(request))

  def processMessages(): Unit = incoming.pullWhile (
    response => {
      val now = System.currentTimeMillis
      sentBuffer.pull match {
        case PullResult.Item(source) => {
          val tags = hpTags ++ tagDecorator.tagsFor(source.message, response)
          latency.add(tags = tags, value = (now - source.queueTime).toInt)
          transitTime.add(tags = tags, value = (now - source.sendTime).toInt)
          queueTime.add(tags = tags, value = (source.sendTime - source.queueTime).toInt)
          source.complete(Success(response))
          requests.hit(tags = tags)
        }
        case PullResult.Empty(_) => {
          throw new DataException(s"No Request for response ${response.toString}!")
        }
        case other => {
          throw new DataException(s"Invalid state on sent buffer $other")
        }
      }
      checkGracefulDisconnect()
      PullAction.PullContinue
    },
    _ => ()
  )

  override def connected() {
    log.info(s"$id Connected to $address")
    clientState = ClientState.Connected
    retryIncident = None
    processMessages()
  }

  private def purgeBuffers(reason : Throwable) {
    sentBuffer.filterScan { s => failRequest(s, reason); true }
    if (failFast) {
      pending.filterScan{ s => failRequest(s, reason); true }
    }
  }

  protected def connectionClosed(cause: DisconnectCause): Unit = {
    clientState = ClientState.Terminated
    disconnects.hit(tags = hpTags + ("cause" -> cause.tagString))
    purgeBuffers(new NotConnectedException(s"${cause.logString}"))
  }

  protected def connectionLost(cause : DisconnectError) {
    cause match {
      case DisconnectCause.ConnectFailed(error) => {
        log.warning(s"$id failed to connect to ${address.toString}: ${error.getMessage}")
        connectionFailures.hit(tags = hpTags)
        purgeBuffers(new NotConnectedException(s"${cause.logString}"))
      }
      case _ => {
        log.warning(s"$id connection lost to ${address.toString}: ${cause.logString}")
        disconnects.hit(tags = hpTags + ("cause" -> cause.tagString))
        purgeBuffers(new ConnectionLostException(s"${cause.logString}"))
      }
    }
    attemptReconnect()
  }

  override protected def onConnectionTerminated(reason: DisconnectCause) {
    reason match {
      case error: DisconnectError => connectionLost(error)
      case _ => connectionClosed(reason)
    }
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
    if (clientState == ClientState.ShuttingDown && sentBuffer.length == 0 && pending.length == 0) {
      upstream.shutdown()
    }
  }

  override def onFatalError(reason: Throwable) = {
    worker ! Kill(id, DisconnectCause.Error(reason))
    None
  }


  override protected def onIdleCheck(period: FiniteDuration) {
    val now = System.currentTimeMillis
    if (sentBuffer.length > 0 && sentBuffer.head.isTimedOut(now)) {
      // the oldest sent message has expired with no response - kill the connection
      // sending the Kill message instead of disconnecting will trigger the reconnection logic
      //TODO : We can probably just fail the request but keep the item buffered and not have to kill the connection
      worker ! Kill(id, DisconnectCause.TimedOut)
    }
    //remove any pending messages that have timed out.
    pending.filterScan{ pending =>
      if (pending.isTimedOut(now)) {
        failRequest(pending, new RequestTimeoutException)
        true
      } else false
    }
  }

  private def failRequest(request: SourcedRequest, exception: Throwable): Unit = {
    errors.hit(tags = hpTags + ("type" -> exception.metricsName))
    request.complete(Failure(exception))
  }

  def disconnect() {
    connection.disconnect()
  }
}
