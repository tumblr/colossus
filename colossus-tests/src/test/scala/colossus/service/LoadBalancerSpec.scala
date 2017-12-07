package colossus.service

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import colossus.controller.Codec
import colossus.core.BackoffMultiplier.Constant
import colossus.core.{BackoffPolicy, ConnectionStatus, NoRetry, WorkerRef}
import colossus.metrics.MetricAddress
import colossus.protocols.http.{Http, HttpRequest, HttpResponse}
import colossus.testkit.{CallbackAwait, ColossusSpec, FakeIOSystem}
import org.scalatest.mockito.MockitoSugar

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class LoadBalancerSpec extends ColossusSpec with MockitoSugar {

  "Load balancer" must {

    implicit val workerRef: WorkerRef = mock[WorkerRef]

    val address1 = new InetSocketAddress(1)
    val address2 = new InetSocketAddress(2)
    val address3 = new InetSocketAddress(3)
    val address4 = new InetSocketAddress(4)
    val address5 = new InetSocketAddress(5)

    val simplePermutationFactory =
      (clients: Seq[Client[Http, Callback]]) =>
        new PermutationGenerator[Client[Http, Callback]] {
          override def next(): List[Client[Http, Callback]] = clients.toList
      }

    "correctly add and remove clients" in {
      val beginningClients = Seq(address1, address2, address3, address3, address4)

      val config = ClientConfig(
        beginningClients,
        1.second,
        MetricAddress("/")
      )

      val (serviceClientFactory, sentToClients, interceptorCount) = createServiceClientFactory()

      val lb = new LoadBalancer[Http](config, serviceClientFactory, simplePermutationFactory)
      lb.addInterceptor(new Interceptor[Http] {
        override def apply(request: HttpRequest, callback: Callback[HttpResponse] => Callback[HttpResponse])
          : (HttpRequest, Callback[HttpResponse] => Callback[HttpResponse]) = {
          (request, callback)
        }
      })

      // each client should have the interceptor added
      assert(interceptorCount.get() == 5)

      val newAddresses = Seq(address1, address2, address2, address3, address5)

      lb.send(HttpRequest.get("/")).execute()

      assert(sentToClients == beginningClients)

      lb.update(newAddresses)

      // new clients should have the interceptor added (only two added)
      assert(interceptorCount.get() == 7)

      sentToClients.clear()

      lb.send(HttpRequest.get("/")).execute()

      assert(sentToClients.map(_.toString).sorted == newAddresses.map(_.toString).sorted)
    }

    "send to only one client when configured with no retry" in {
      val config = ClientConfig(
        Seq(address1, address2, address3),
        1.second,
        MetricAddress("/"),
        requestRetry = NoRetry
      )

      val (serviceClientFactory, sentToClients, _) = createServiceClientFactory()

      val lb = new LoadBalancer[Http](config, serviceClientFactory, simplePermutationFactory)

      lb.send(HttpRequest.get("/")).execute()

      assert(sentToClients == Seq(address1))
    }

    "send to all clients when no max tries specified" in {
      val clients = Seq(address1, address2, address3)

      val config = ClientConfig(
        clients,
        1.second,
        MetricAddress("/"),
        requestRetry = BackoffPolicy(
          baseBackoff = 0.second,
          multiplier = Constant,
          maxTries = None
        )
      )

      val (serviceClientFactory, sentToClients, _) = createServiceClientFactory()

      val lb = new LoadBalancer[Http](config, serviceClientFactory, simplePermutationFactory)

      lb.send(HttpRequest.get("/")).execute()

      assert(sentToClients == clients)
    }

    "send to X clients when max tries specified" in {
      val config = ClientConfig(
        Seq(address1, address2, address3),
        1.second,
        MetricAddress("/"),
        requestRetry = BackoffPolicy(
          baseBackoff = 0.second,
          multiplier = Constant,
          maxTries = Some(2)
        )
      )

      val (serviceClientFactory, sentToClients, _) = createServiceClientFactory()

      val lb = new LoadBalancer[Http](config, serviceClientFactory, simplePermutationFactory)

      lb.send(HttpRequest.get("/")).execute()

      assert(sentToClients == Seq(address1, address2))
    }

    "send to the same connection when retries is set higher than connections available" in {
      val config = ClientConfig(
        Seq(address1),
        1.second,
        MetricAddress("/"),
        requestRetry = BackoffPolicy(
          baseBackoff = 0.second,
          multiplier = Constant,
          maxTries = Some(3)
        )
      )

      val (serviceClientFactory, sentToClients, _) = createServiceClientFactory()

      val lb = new LoadBalancer[Http](config, serviceClientFactory, simplePermutationFactory)

      lb.send(HttpRequest.get("/")).execute()

      assert(sentToClients == Seq(address1, address1, address1))
    }

    "return failure when no clients provided" in {
      val config = ClientConfig(
        Seq(),
        1.second,
        MetricAddress("/")
      )

      val (serviceClientFactory, sentToClients, _) = createServiceClientFactory()

      val lb = new LoadBalancer[Http](config, serviceClientFactory, simplePermutationFactory)

      val callback = lb.send(HttpRequest.get("/"))

      implicit val callbackExecutor: CallbackExecutor = FakeIOSystem.testExecutor

      intercept[SendFailedException] {
        CallbackAwait.result(callback, 1.second)
      }

      assert(sentToClients.isEmpty)
    }
  }

  def createServiceClientFactory(): (ServiceClientFactory[Http], ArrayBuffer[InetSocketAddress], AtomicInteger) = {
    val sentToClients = ArrayBuffer.empty[InetSocketAddress]

    var interceptorCount = new AtomicInteger(0)
    var disconnectCount  = new AtomicInteger(0)

    val serviceClientFactory = new ServiceClientFactory[Http] {
      override implicit def clientTagDecorator: TagDecorator[Http] = ???

      override def codecProvider: Codec.Client[Http] = ???

      override def defaultName: String = ???

      override def createClient(config: ClientConfig, clientAddress: InetSocketAddress)(
          implicit worker: WorkerRef): Client[Http, Callback] = {
        new Client[Http, Callback] {
          override def connectionStatus: Callback[ConnectionStatus] = ???

          override def send(input: HttpRequest): Callback[HttpResponse] = {
            sentToClients.append(clientAddress)
            Callback.failed(new Exception("fake"))
          }

          override def disconnect(): Unit = {}

          override def addInterceptor(interceptor: Interceptor[Http]): Unit = {
            interceptorCount.incrementAndGet()
          }

          override def address(): InetSocketAddress = clientAddress

          override def update(addresses: Seq[InetSocketAddress]): Unit = {}
        }
      }
    }

    (serviceClientFactory, sentToClients, interceptorCount)
  }
}
