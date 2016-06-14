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


object TestNamespace {

  def apply(): MetricNamespace = MetricContext("/", Collection.withReferenceConf(Seq(1.second)), Map())

}

