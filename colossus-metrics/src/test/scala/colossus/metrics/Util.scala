package colossus
package metrics

import akka.actor._
import akka.testkit._
import akka.util.Timeout
import org.scalatest._
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


abstract class MetricIntegrationSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("Spec"))

  implicit val timeout = Timeout(500.milliseconds)

  implicit val mySystem = system


  override protected def afterAll() {
    TestKit.shutdownActorSystem(system)
  }
}

class EventuallyEqualMatcher[T](expected : T, tries : Int, timeout : FiniteDuration, retryInterval : FiniteDuration) extends Matcher[() => Future[T]] {

  override def apply(lf: () => Future[T]): MatchResult = {

    var remainingTries = tries
    var last = Await.result(lf(), timeout)
    var notMatches = last != expected && remainingTries > 0
    while(notMatches) {
      Thread.sleep(retryInterval.toMillis)
      remainingTries -= 1
      last = Await.result(lf(), timeout)
      notMatches = last != expected && remainingTries > 0
    }
    val notMatchMsg = s"Failed to eventually match $expected after $tries tries.  Last value was $last"
    MatchResult(!notMatches, notMatchMsg, notMatchMsg)
  }
}


trait EventuallyEquals {

  def eventuallyEqual[T](expected : T, tries : Int = 5, timeout: FiniteDuration = 500.milliseconds, retryInterval : FiniteDuration = 100.milliseconds) = new EventuallyEqualMatcher[T](expected, tries, timeout, retryInterval)

}

object EventuallyEquals extends EventuallyEquals


trait MetricSystemMatchers{

  import akka.pattern.ask
  import colossus.metrics.IntervalAggregator._

  private def listRegistrants(m : MetricSystem, msg : Any)(implicit ac : ActorSystem, fd : FiniteDuration) : () => Future[Map[ActorRef, Set[ActorRef]]] = {
    implicit val ec = ac.dispatcher
    () => {
      Future.sequence(m.metricIntervals.values.map(x => aggregatorRegistrants(x.intervalAggregator, msg))).map(_.toMap)
    }
  }
  private def aggregatorRegistrants(agg : ActorRef, msg : Any)(implicit ac : ActorSystem, fd : FiniteDuration) : Future[(ActorRef,Set[ActorRef])] = {
    implicit val ec = ac.dispatcher
    implicit val to  = Timeout(fd)
    (agg ? msg).mapTo[Set[ActorRef]].map(x => agg->x)
  }

  class ContainsCollectors(expected : Set[ActorRef], tries : Int, timeout : FiniteDuration, retryInterval : FiniteDuration)
                          (implicit ac : ActorSystem) extends  Matcher[MetricSystem] {

    override def apply(m: MetricSystem): MatchResult = {
      implicit val fd = timeout
      val l = listRegistrants(m, ListCollectors)
      val map = m.metricIntervals.values.map(x => x.intervalAggregator->expected).toMap
      new EventuallyEqualMatcher[Map[ActorRef, Set[ActorRef]]](map, tries, timeout, retryInterval).apply(l)
    }

  }

  class ContainsReporters(expected : Map[ActorRef, Set[ActorRef]], tries : Int, timeout : FiniteDuration, retryInterval : FiniteDuration)
                         (implicit ac : ActorSystem) extends  Matcher[MetricSystem] {
    override def apply(m: MetricSystem): MatchResult = {
      implicit val fd = timeout
      val l = listRegistrants(m, ListReporters)
      new EventuallyEqualMatcher[Map[ActorRef, Set[ActorRef]]](expected, tries, timeout, retryInterval).apply(l)
    }
  }


  def haveCollectors(collectors : Set[ActorRef], tries : Int = 5, timeout: FiniteDuration = 500.milliseconds, retryInterval : FiniteDuration = 100.milliseconds)(implicit ac : ActorSystem) = {
    new ContainsCollectors(collectors, tries, timeout, retryInterval)
  }

  def haveReporters(reporters : Map[ActorRef, Set[ActorRef]], tries : Int = 5, timeout: FiniteDuration = 500.milliseconds, retryInterval : FiniteDuration = 100.milliseconds)(implicit ac : ActorSystem) = {
    new ContainsReporters(reporters, tries, timeout, retryInterval)
  }
}

object MetricSystemMatchers extends MetricSystemMatchers
