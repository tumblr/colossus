package colossus.metrics

import akka.actor._
import akka.agent.Agent
import akka.pattern.ask

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.util.Timeout
import MetricAddress.Root


case class MetricSystemId(id: Long)


case class MetricSystem(id: MetricSystemId, namespace: MetricAddress, clock: ActorRef, database: ActorRef, snapshot: Agent[MetricMap], tickPeriod: FiniteDuration) {


  def query(filter: MetricFilter): MetricMap = snapshot().filter(filter)  

  def query(queryString: String): MetricMap = query(MetricFilter(queryString))

  def report(config: MetricReporterConfig)(implicit fact: ActorRefFactory): ActorRef = MetricReporter(config)(this, fact)

  def sharedCollection(globalTags: TagMap = TagMap.Empty)(implicit fact: ActorRefFactory) = {
    implicit val me = this
    SharedCollection(globalTags)
  }

  def last: MetricMap = snapshot()

}

object MetricSystem {
  def apply(namespace: MetricAddress, tickPeriod: FiniteDuration = 1.second, collectSystemMetrics: Boolean = true)
  (implicit system: ActorSystem): MetricSystem = {
    import system.dispatcher
    val id = MetricSystemId(System.nanoTime)
    val clock = system.actorOf(Props(classOf[MetricClock], id, tickPeriod), name =  s"${namespace.idString}-clock")
    val snap = Agent[MetricMap](Map())
    val db = system.actorOf(Props(classOf[MetricDatabase], id, namespace, snap, collectSystemMetrics))

    val metrics = MetricSystem(id, namespace, clock, db, snap, tickPeriod)

    metrics
  }

  def deadSystem(implicit system: ActorSystem) = {
    import ExecutionContext.Implicits.global //this is ok since we're using it to create an agent that's never used
    MetricSystem(MetricSystemId(System.nanoTime), Root / "DEAD", system.deadLetters, system.deadLetters, Agent[MetricMap](Map()), 0.seconds)
  }

  object Global {
    //coming soon
  }
}



