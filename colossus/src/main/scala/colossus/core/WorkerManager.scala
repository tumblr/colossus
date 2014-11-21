package colossus
package core

import akka.actor._
import akka.pattern.ask
import akka.routing.RoundRobinGroup
import akka.util.Timeout

import metrics.MetricSystem

import java.net.InetSocketAddress

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable.Iterable
import scala.util.{Failure, Success}


private[colossus] case class WorkerManagerConfig(numWorkers: Int, metrics: MetricSystem)

/**
 * A WorkerManager is just that, an Actor who is responsible for managing all of the Worker Actors in an IOSystem.
 * It is responsible for creating, killing, restarting, relaying messages, etc.
 *
 * @param config configuration parameters
 */
private[colossus] class WorkerManager(config: WorkerManagerConfig) extends Actor with ActorLogging with Stash {
  import config._
  import WorkerManager._
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import context.dispatcher

  /*
   * This strategy is chosen specifically for durability.  We don't want to kill the entire system if workers start
   * excepting.
   */
  override val supervisorStrategy = {
    OneForOneStrategy() {
      case _: Exception => Restart
    }
  }

  //NOTE: private?
  case class State(workers: Seq[ActorRef], system: IOSystem) {
    val workerRouter = context.actorOf(Props.empty.withRouter(RoundRobinGroup(Iterable(workers.map(_.path.toString) : _*))))
  }

  var currentState: Option[State] = None

  
  //we store the RegisterServer object and not just the ServerRef because we
  //need to be able to send the delegatorFactory to any new/restarted workers
  var registeredServers = collection.mutable.ArrayBuffer[RegisterServer]()

  //this is used when the manager receives a Connect request to round-robin across workers
  var nextConnectIndex = 0

  var latestSummary: Seq[ConnectionInfo] = Nil
  var latestSummaryTime = 0L


  def startingUp: Receive = {
    case ReadyCheck => {
      sender ! WorkersNotReady
    }
  }

  def receive = startingUp orElse {
    case Initialize(system) => {
      val workers = (1 to numWorkers).map{i =>
        val workerConfig = WorkerConfig(
          workerId = i,
          io = system
        )
        val worker = context.actorOf(Props(classOf[Worker],workerConfig ).withDispatcher("server-dispatcher"), name = s"worker-$i")
        context.watch(worker)
        worker
      }
      val state = State(workers, system)
      currentState = Some(state)
      context.become(waitForWorkers(state, 0))
    }
    case other => stash() //unstash happens when we enter the running state in waitForWorkers
  }

  

  def waitForWorkers(state: State, ready: Int): Receive = startingUp orElse {
    case WorkerReady => {
      val nowReady = ready + 1
      if (nowReady == numWorkers) {
        log.info("All Workers reports ready, lets do this")
        context.system.scheduler.schedule(1.seconds, 1.seconds, self, IdleCheck)
        unstashAll()
        context.become(running(state))
      } else {
        context.become(waitForWorkers(state, nowReady))
      }
    }
    case other => stash()
  }

  def running(state: State): Receive = {
    var nextWorkerIndex = 0
    def nextWorker = {
      nextWorkerIndex += 1
      if (nextWorkerIndex >= state.workers.size) {
        nextWorkerIndex = 0
      }
      state.workers(nextWorkerIndex)
    }
    serverRegistration orElse {
      case ReadyCheck => {
        sender ! WorkersReady(state.workerRouter)
      }
      case IdleCheck => {
        state.workers.foreach{_ ! Worker.CheckIdleConnections}
      }
      case WorkerCommand.Schedule(in, cmd) => context.system.scheduler.scheduleOnce(in, sender(), cmd)
      case GatherConnectionInfo(rOpt) => {
        implicit val timeout = Timeout(50.milliseconds)
        Future
          .traverse(state.workers){worker => (worker ? Worker.ConnectionSummaryRequest).mapTo[Worker.ConnectionSummary]}
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
      case WorkerReady => {
        log.warning("Received Ready Notification from new/restarted worker")
        registeredServers.foreach{sender ! _}
      }
    }
      
  }

  def serverRegistration: Receive = {
    case r : RegisterServer => {
      implicit val timeout = Timeout(r.server.config.settings.delegatorCreationDuration.interval)
      currentState.foreach(registerServer(_, r))
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
  }

  private def registerServer(state : State, r : RegisterServer)(implicit to : Timeout) {
    log.debug(s"attempting to register ${r.server.name}")
    val s = Future.traverse(state.workers){ _ ? r }
    s.onComplete {
      case Success(x) if !x.contains(RegistrationFailed) => {
        //closing over state..kind of a no no, if an "unregister" request comes through while a registration is processing.
        //That's an oddball state however.
        //this is only additive, so i'm kind of ok with this, until we actually need to change it.
        registeredServers.append(r)
        context.watch(r.server.server)
        r.server.server ! WorkersReady(state.workerRouter)
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
    val maxTries = r.server.config.settings.delegatorCreationDuration.maximumTries
    val tryAgain = maxTries.fold(true){_ < r.timesTried}
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
    currentState.foreach{_.workers.foreach{worker =>
      worker ! u
    }}
  }

  override def postStop() {
    currentState.foreach{
      _.workers.foreach{_ ! PoisonPill}
    }
  }
}

private[colossus] object WorkerManager {
  case class WorkersReady(workerRouter: ActorRef)
  case object WorkersNotReady

  //send from workers to the manager
  private[colossus] case object WorkerReady
  private[colossus] case object ServerRegistered
  private[colossus] case object RegistrationFailed

  //ping manager
  case class RegisterServer(server: ServerRef, factory: Delegator.Factory, timesTried : Int = 1)
  case class UnregisterServer(server: ServerRef)
  case object ListRegisteredServers

  case class RegisteredServers(servers : Seq[ServerRef])

  private[colossus] case class Initialize(system: IOSystem)
  private[colossus] case class GatherConnectionInfo(requester: Option[ActorRef])

  //sent from a worker during restart
  private[colossus] case object ReinitializeWorker

  case object ReadyCheck
  case object IdleCheck
  case object Shutdown
  case object Apocalypse
  case class Connect(address: InetSocketAddress)

  case object GetConnectionSummary
}
