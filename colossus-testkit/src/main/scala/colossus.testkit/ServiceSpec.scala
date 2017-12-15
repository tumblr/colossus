package colossus.testkit

import colossus.metrics.MetricSystem

import scala.concurrent.Await
import scala.concurrent.duration._
import java.net.InetSocketAddress

import colossus.core.{IOSystem, ServerRef}
import colossus.service.{ClientConfig, FutureClient, FutureClientFactory, Protocol}

import scala.reflect.ClassTag

abstract class ServiceSpec[P <: Protocol](implicit clientFactory: FutureClientFactory[P]) extends ColossusSpec {

  type Request  = P#Request
  type Response = P#Response

  implicit val ioSystem: IOSystem = IOSystem("test-system", Some(2), MetricSystem.deadSystem)

  def service: ServerRef

  def requestTimeout: FiniteDuration = 5.seconds

  //this must be lazy, or we get crazy order of instanation related issues in all subclasses.
  lazy val runningService: ServerRef = {
    val serviceRef = service
    waitForServer(serviceRef)
    serviceRef
  }

  def clientConfig(timeout: FiniteDuration) = ClientConfig(
    name = "/test-client",
    address = Seq(new InetSocketAddress("localhost", runningService.config.settings.port)),
    requestTimeout = timeout
  )

  def client(timeout: FiniteDuration = requestTimeout) = clientFactory(clientConfig(timeout))

  def withClient(f: FutureClient[P] => Unit): Unit = {
    val c = client()
    f(c)
    c.disconnect()
  }

  def expectResponse(request: Request, response: Response): Unit = {
    withClient { client =>
      try {
        Await.result(client.send(request), requestTimeout) must equal(response)
      } catch {
        case _: java.util.concurrent.TimeoutException =>
          fail(s"Timed out waiting for a response after $requestTimeout")
      }
    }
  }

  def expectResponseType[T <: Response: ClassTag](request: Request): Unit = {
    withClient { client =>
      Await.result(client.send(request), requestTimeout) match {
        case t: T  =>
        case other => fail("Wrong type for response ${other}")
      }
    }
  }
}
