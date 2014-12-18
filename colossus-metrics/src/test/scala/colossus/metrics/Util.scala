package colossus
package metrics

import akka.testkit._
import akka.actor._
import akka.util.Timeout
import scala.concurrent.duration._
import org.scalatest._


abstract class MetricIntegrationSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("Spec"))

  implicit val timeout = Timeout(500.milliseconds)

  implicit val mySystem = system


  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }
}
