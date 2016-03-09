package colossus
package core

import akka.actor._
import akka.agent.Agent
import akka.pattern.ask
import akka.routing.RoundRobinGroup
import akka.util.Timeout

import metrics.MetricSystem

import java.net.InetSocketAddress

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable.Iterable
import scala.util.{Failure, Success}



/**
 * A WorkerManager is just that, an Actor who is responsible for managing all of the Worker Actors in an IOSystem.
 * It is responsible for creating, killing, restarting, relaying messages, etc.
 *
 * @param config configuration parameters
 */
private[colossus] class WorkerManager(workerAgent: Agent[IndexedSeq[WorkerRef]], ioSystem: IOSystem) 
extends Actor with ActorLogging with Stash {
  import WorkerManager._
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import context.dispatcher

  import ioSystem.config.numWorkers


  /*
   * This strategy is chosen specifically for durability.  We don't want to kill the entire system if workers start
   * excepting.
   */
  override val supervisorStrategy = {
    OneForOneStrategy() {
      case _: Exception => Restart
    }
  }

  val workers = (1 to numWorkers).map{i =>
    val workerConfig = WorkerConfig(
      workerId = i,
      io = ioSystem
    )
    val worker = context.actorOf(Props(classOf[Worker],workerConfig ).withDispatcher("server-dispatcher"), name = s"worker-$i")
    context.watch(worker)
    worker
  }
  val workerRouter = context.actorOf(Props.empty.withRouter(RoundRobinGroup(Iterable(workers.map(_.path.toString) : _*))))


  //we store the RegisterServer object and not just the ServerRef because we
  //need to be able to send the delegatorFactory to any new/restarted workers
  var registeredServers = collection.mutable.ArrayBuffer[RegisterServer]()

  //this is used when the manager receives a Connect request to round-robin across workers
  var nextConnectIndex = 0

  var latestSummary: Seq[ConnectionSnapshot] = Nil
  var latestSummaryTime = 0L

  def receive = waitForWorkers(Vector())

  def waitForWorkers(ready: Vector[WorkerRef]): Receive = {
    case ReadyCheck => {
      sender ! WorkersNotReady
    }
    case WorkerReady(worker) => {
      val nowReady = ready :+ worker
      if (nowReady.size == numWorkers) {
        log.info("All Workers reports ready, lets do this")
        workerAgent alter{_ => nowReady}
        context.system.scheduler.schedule(IdleCheckFrequency, IdleCheckFrequency, self, IdleCheck)
        unstashAll()
        context.become(running)
      } else {
        context.become(waitForWorkers(nowReady))
      }
    }
    case other => stash()
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
      case ReadyCheck => {
        sender ! WorkersReady(workerRouter)
      }
      case IdleCheck => {
        workers.foreach{_ ! Worker.CheckIdleConnections}
      }
      case WorkerCommand.Schedule(in, cmd) => context.system.scheduler.scheduleOnce(in, sender(), cmd)
      case GatherConnectionInfo(rOpt) => {
        implicit val timeout = Timeout(50.milliseconds)
        Future
          .traverse(workers){worker => (worker ? Worker.ConnectionSummaryRequest).mapTo[Worker.ConnectionSummary]}
          .map{seqs =>
            val summary = Worker.ConnectionSummary(seqs.flatMap{_.infos})
            self ! summary
            rOpt.foreach{requester => requester ! summary}
          }
      }
      case Worker.ConnectionSummary(sum) => {
        latestSummary = sum
        latestSummaryTime = System.currentTimeMillis
        log.debug(s"Got connection summary, size ${sum.size}")
      }
      case GetConnectionSummary => {
        val r = sender()
        if (System.currentTimeMillis - latestSummaryTime > 5000) {
          self ! GatherConnectionInfo(Some(r))
        } else {
          sender ! Worker.ConnectionSummary(latestSummary)
        }
      }
      case Shutdown => self ! PoisonPill
      case Apocalypse => {
        log.info("SHUT DOWN EVERYTHING")
        context.system.shutdown()
      }
      case c: IOCommand => nextWorker ! c
      case WorkerReady(worker) => {
        log.warning("Received Ready Notification from new/restarted worker")
        registeredServers.foreach{sender ! _}
      }
    }

  }

  def serverRegistration: Receive = {
    case r : RegisterServer => {
      implicit val timeout = Timeout(r.server.config.settings.delegatorCreationDuration.interval)
      registerServer(r)
    }
    case u: UnregisterServer => {
      val registeredServer = registeredServers.find(_.server == u.server)
      registeredServer.fold(log.warning(s"Attempted to Unregister unknown server ${u.server.name}"))(unregisterServer(u, _))
    }

    //should be only triggered when a Server actor terminates
    case Terminated(ref) => {
        val registeredServer = registeredServers.find(_.server.server == ref)
        registeredServer.fold { log.warning(s"$ref was terminated, and is not a registered server.")}{ r =>
            unregisterServer(UnregisterServer(r.server), r)
        }
    }

    case ListRegisteredServers => {
      sender ! RegisteredServers(registeredServers.map(_.server))
    }

    case s:  ServerShutdownRequest => workers.foreach{_ ! s}
  }

  private def registerServer(r : RegisterServer)(implicit to : Timeout) {
    log.debug(s"attempting to register ${r.server.name}")
    val s = Future.traverse(workers){ _ ? r }
    s.onComplete {
      case Success(x) if !x.contains(RegistrationFailed) => {
        //closing over state..kind of a no no, if an "unregister" request comes through while a registration is processing.
        //That's an oddball state however.
        //this is only additive, so i'm kind of ok with this, until we actually need to change it.
        registeredServers.append(r)
        context.watch(r.server.server)
        r.server.server ! WorkersReady(workerRouter)
      }
      case Failure(err)  => {
        log.error(err, s"Worker failed to register server ${r.server.name} after ${r.timesTried} tries with error: ${err.getMessage}")
        tryReregister(r)
      }
      case _ => {
        log.error(s"One or more Workers failed to register server ${r.server.name} after ${r.timesTried} tries")
        tryReregister(r)
      }
    }
  }

  private def tryReregister(r : RegisterServer) {
    val tryAgain = r.server.config.settings.delegatorCreationDuration.isExpended(r.timesTried)
    if(tryAgain) {
      self ! r.copy(timesTried = r.timesTried + 1)
    }else {
      log.error(s"Exhausted all attempts to register ${r.server.name}, aborting.")
      r.server.server ! RegistrationFailed
    }
  }


  private def unregisterServer(u : UnregisterServer, r : RegisterServer) {
    log.info(s"unregistering server: ${r.server.name}")
    registeredServers -= r
    workers.foreach{worker =>
      worker ! u
    }
  }

  override def postStop() {
    workers.foreach{_ ! PoisonPill}
  }
}

private[colossus] object WorkerManager {
  case class WorkersReady(workerRouter: ActorRef)
  case object WorkersNotReady

  //send from workers to the manager
  private[colossus] case class WorkerReady(worker: WorkerRef)
  private[colossus] case object ServerRegistered
  private[colossus] case object RegistrationFailed

  //ping manager
  case class RegisterServer(server: ServerRef, factory: Delegator.Factory, timesTried : Int = 1)
  case class UnregisterServer(server: ServerRef)

  //sent by the server and broadcast to all workers when the server is
  //beginning to shutdown.  This initiates a shutdown request on all
  //connections
  case class ServerShutdownRequest(server: ServerRef)

  case object ListRegisteredServers

  case class RegisteredServers(servers : Seq[ServerRef])

  private[colossus] case class GatherConnectionInfo(requester: Option[ActorRef])

  //sent from a worker during restart
  private[colossus] case object ReinitializeWorker

  case object ReadyCheck
  case object IdleCheck
  case object Shutdown
  case object Apocalypse
  case class Connect(address: InetSocketAddress)

  case object GetConnectionSummary


  val IdleCheckFrequency = 100.milliseconds
}
