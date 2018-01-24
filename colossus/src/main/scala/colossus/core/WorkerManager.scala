package colossus.core

import akka.pattern.ask
import akka.routing.RoundRobinGroup
import akka.util.Timeout
import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, Stash, Terminated}
import akka.actor.SupervisorStrategy.Restart
import colossus.metrics.logging.ColossusLogging

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable.Iterable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

/**
  * A WorkerManager is an Actor that is responsible for managing all of the Worker Actors in an IOSystem.
  * It is responsible for creating, killing, restarting, relaying messages, etc.
  *
  * @param workerRefs WorkerRefs that this WorkerManager manages
  * @param ioSystem Containing IOSystem
  * @param workerFactory Worker factory for creating workers
  */
private[colossus] class WorkerManager(workerRefs: IOSystem.WorkerRefs, ioSystem: IOSystem, workerFactory: WorkerFactory)
    extends Actor
    with ColossusLogging
    with Stash {

  import WorkerManager._

  import context.dispatcher

  /*
   * This strategy is chosen specifically for durability.  We don't want to kill the entire system if workers start
   * excepting.
   */
  override val supervisorStrategy: OneForOneStrategy = {
    OneForOneStrategy() {
      case e: Exception =>
        // TODO default is to restart on Exception... adding logging to see if this code gets hit
        error(s"[${ioSystem.name}] Supervisor strategy error $e")
        Restart
    }
  }

  val workers: Seq[ActorRef] = (1 to ioSystem.numWorkers).map { i =>
    workerFactory.createWorker(i, ioSystem, context)
  }

  val workerRouter: ActorRef = context.actorOf(
    Props.empty.withRouter(RoundRobinGroup(Iterable(workers.map(_.path.toString): _*)))
  )

  private var registeredServers         = ArrayBuffer.empty[ServerRef]
  private var latestSummary             = Seq.empty[ConnectionSnapshot]
  private var latestSummaryTime         = 0L
  private var outstandingWorkerIdleAcks = 0

  def receive: Receive = waitForWorkers(Vector())

  def waitForWorkers(ready: Vector[WorkerRef]): Receive = {
    case ReadyCheck =>
      sender ! WorkersNotReady

    case WorkerReady(worker) =>
      val nowReady = ready :+ worker
      if (nowReady.size == ioSystem.numWorkers) {
        info(s"[${ioSystem.name}] All Workers reports ready, lets do this")
        workerRefs.set(nowReady)
        context.system.scheduler.scheduleOnce(IdleCheckFrequency, self, IdleCheck)
        unstashAll()
        context.become(running)
      } else {
        context.become(waitForWorkers(nowReady))
      }

    case _ =>
      stash()
  }

  def running: Receive = {
    var nextWorkerIndex = 0
    def nextWorker = {
      nextWorkerIndex += 1
      if (nextWorkerIndex >= workers.size) {
        nextWorkerIndex = 0
      }
      workers(nextWorkerIndex)
    }

    serverRegistration orElse {
      case ReadyCheck =>
        sender ! WorkersReady(workerRouter)

      case IdleCheck =>
        outstandingWorkerIdleAcks = workers.size
        workers.foreach { _ ! Worker.CheckIdleConnections }

      case IdleCheckExecuted =>
        outstandingWorkerIdleAcks -= 1
        if (outstandingWorkerIdleAcks == 0) {
          context.system.scheduler.scheduleOnce(IdleCheckFrequency, self, IdleCheck)
        }

      case WorkerCommand.Schedule(in, cmd) =>
        context.system.scheduler.scheduleOnce(in, sender(), cmd)

      case GatherConnectionInfo(rOpt) =>
        implicit val timeout: Timeout = Timeout(50.milliseconds)
        Future
          .traverse(workers) { worker =>
            (worker ? Worker.ConnectionSummaryRequest).mapTo[Worker.ConnectionSummary]
          }
          .map { seqs =>
            val summary = Worker.ConnectionSummary(seqs.flatMap { _.infos })
            self ! summary
            rOpt.foreach { requester =>
              requester ! summary
            }
          }

      case Worker.ConnectionSummary(sum) =>
        latestSummary = sum
        latestSummaryTime = System.currentTimeMillis
        debug(s"[${ioSystem.name}] Got connection summary, size ${sum.size}")

      case GetConnectionSummary =>
        val r = sender()
        if (System.currentTimeMillis - latestSummaryTime > 5000) {
          self ! GatherConnectionInfo(Some(r))
        } else {
          sender ! Worker.ConnectionSummary(latestSummary)
        }

      case Shutdown =>
        self ! PoisonPill

      case Apocalypse =>
        info(s"[${ioSystem.name}] Shutting down")
        context.system.terminate()

      case c: IOCommand =>
        nextWorker ! c

      case WorkerReady(_) =>
        warn(s"[${ioSystem.name}] Received ready notification from new/restarted worker")
        registeredServers.foreach { sender ! _ }
    }

  }

  def serverRegistration: Receive = {
    case RegisterServer(server) =>
      registerServer(server, None)

    case AttemptRegisterServer(server, incident) =>
      registerServer(server, Some(incident))

    case RegistrationSucceeded(server) =>
      registeredServers.append(server)
      context.watch(server.server)
      server.server ! WorkersReady(workerRouter)

    case UnregisterServer(server) =>
      registeredServers.find(_ == server) match {
        case Some(found) => unregisterServer(found)
        case None        => warn(s"[${ioSystem.name}] Attempted to unregister unknown server ${server.name}")
      }

    //should be only triggered when a Server actor terminates
    case Terminated(ref) =>
      registeredServers.find(_.server == ref) match {
        case Some(found) => unregisterServer(found)
        case None        => warn(s"[${ioSystem.name}] Received terminated signal for unregistered server $ref")
      }

    case ListRegisteredServers =>
      sender ! RegisteredServers(registeredServers)

    case s: ServerShutdownRequest =>
      workers.foreach { _ ! s }
  }

  private def registerServer(server: ServerRef, retry: Option[RetryIncident]) {
    debug(s"[${ioSystem.name}] Attempting to register ${server.name}")
    implicit val timeout: Timeout = Timeout(server.config.settings.delegatorCreationPolicy.waitTime)

    val serverFutures = Future.traverse(workers) { _ ? RegisterServer(server) }

    serverFutures.onComplete {
      case Success(x) if !x.contains(RegistrationFailed) =>
        self ! RegistrationSucceeded(server)

      case Failure(err) =>
        retryRegister(err.getMessage)

      case _ =>
        //the error itself is logged by the delegator(serverFutures) that failed
        retryRegister(s"One or more workers failed during registration")

    }

    def retryRegister(message: String) = {
      val incident = retry.getOrElse(server.config.settings.delegatorCreationPolicy.retryPolicy.start())

      val fullMessage = s"Failed to register server ${server.name} after ${incident.attempts} attempts:"

      incident.nextAttempt() match {
        case RetryAttempt.Stop =>
          error(s"[${ioSystem.name}] $fullMessage, aborting")
          server.server ! RegistrationFailed

        case RetryAttempt.RetryNow =>
          error(s"[${ioSystem.name}] $fullMessage, retrying now")
          self ! AttemptRegisterServer(server, incident)

        case RetryAttempt.RetryIn(time) =>
          error(s"[${ioSystem.name}] $fullMessage, retrying in $time")
          context.system.scheduler.scheduleOnce(time, self, AttemptRegisterServer(server, incident))

      }
    }
  }

  private def unregisterServer(server: ServerRef) {
    info(s"[${ioSystem.name}] Unregistering server: ${server.name}")
    registeredServers -= server
    workers.foreach { worker =>
      worker ! UnregisterServer(server)
    }
  }

  override def postStop() {
    workers.foreach { _ ! PoisonPill }
  }
}

private[colossus] object WorkerManager {

  def props(workerAgent: IOSystem.WorkerRefs, ioSystem: IOSystem, workerFactory: WorkerFactory): Props = {
    Props(new WorkerManager(workerAgent, ioSystem, workerFactory))
  }

  case class WorkersReady(workerRouter: ActorRef)
  case object WorkersNotReady

  //send from workers to the manager
  private[colossus] case class WorkerReady(worker: WorkerRef)
  private[colossus] case object ServerRegistered
  private[colossus] case object RegistrationFailed

  /**
    * The manager sends this to itself to retry registering a server
    */
  private[colossus] case class AttemptRegisterServer(server: ServerRef, retry: RetryIncident)

  /**
    * The manager sends this to itself when the registration for the given server
    * (which happens asynchronously via futures) is complete
    */
  private[colossus] case class RegistrationSucceeded(server: ServerRef)

  //ping manager
  case class RegisterServer(server: ServerRef)
  case class UnregisterServer(server: ServerRef)

  //sent by the server and broadcast to all workers when the server is
  //beginning to shutdown.  This initiates a shutdown request on all
  //connections
  case class ServerShutdownRequest(server: ServerRef)

  case object ListRegisteredServers

  case class RegisteredServers(servers: Seq[ServerRef])

  private[colossus] case class GatherConnectionInfo(requester: Option[ActorRef])

  //sent from a worker during restart
  private[colossus] case object ReinitializeWorker

  case object ReadyCheck
  case object IdleCheck
  case object Shutdown
  case object Apocalypse
  case class Connect(address: InetSocketAddress)

  case object GetConnectionSummary

  private[colossus] case object IdleCheckExecuted

  val IdleCheckFrequency: FiniteDuration = 100.milliseconds
}
