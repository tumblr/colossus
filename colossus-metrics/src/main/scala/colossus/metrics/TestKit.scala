package colossus.metrics
package testkit

import akka.testkit.TestProbe
import scala.reflect.ClassTag


/**
 * A Fake shared collection for creating event collectors using a provided test probe
 */
class TestSharedCollection(probe: TestProbe) extends Collection[SharedLocality] {

  def getOrAdd[T <: EventCollector : ClassTag, P : Collection.ParamsFor[T]#Type](params: P)(implicit generator: Generator[T,P]): T with SharedLocality = {
    generator.shared(params, CollectorConfig(List()))(probe.ref)
  }
}
