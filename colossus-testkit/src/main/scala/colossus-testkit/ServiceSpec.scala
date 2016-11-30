package colossus
package testkit

import colossus.metrics.MetricSystem

import scala.concurrent.Await
import scala.concurrent.duration._
import java.net.InetSocketAddress

import core.ServerRef
import service._
import scala.reflect.ClassTag

abstract class ServiceSpec[C <: Protocol](implicit provider: ServiceCodecProvider[C], clientProvider: ClientCodecProvider[C]) extends ColossusSpec {

  type Request = C#Input
  type Response = C#Output

  implicit val sys = IOSystem("test-system", Some(2), MetricSystem.deadSystem)


  def service: ServerRef
  def requestTimeout: FiniteDuration

  //this must be lazy, or we get crazy order of instanation related issues in all subclasses.
  lazy val runningService = {
    val s = service
    waitForServer(s)
    s
  }

  def clientConfig(timeout: FiniteDuration) = ClientConfig (
    name = "/test-client",
    address = new InetSocketAddress("localhost", runningService.config.settings.port),
    requestTimeout = timeout
  )

  def client(timeout: FiniteDuration = requestTimeout) = FutureClient[C](clientConfig(timeout))

  def withClient(f: FutureClient[C] => Unit) {
    val c = client()
    f(c)
    c.disconnect()
  }

  def expectResponse(request: Request, response: Response) {
    withClient{client =>
      try {
        Await.result(client.send(request), requestTimeout) must equal(response)
      } catch {
        case timeout: java.util.concurrent.TimeoutException => fail(s"timed out waiting for a response after $requestTimeout")
      }
    }
  }

  def expectResponseType[T <: Response : ClassTag](request: Request) {
    withClient{client =>
      Await.result(client.send(request), requestTimeout) match {
        case t: T => {}
        case other => fail("Wrong type for response ${other}")
      }
    }
  }
}
