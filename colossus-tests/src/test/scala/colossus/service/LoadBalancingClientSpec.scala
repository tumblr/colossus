package colossus
package service

import akka.util.ByteString
import core.{ConnectionState, WorkerCommand}
import colossus.testkit.{ColossusSpec, FakeIOSystem}
import org.scalatest.{WordSpec, MustMatchers}
import org.scalatest.mock.MockitoSugar

import org.mockito.Mockito._

import scala.concurrent.{ExecutionContext, Future, Promise}
import ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import java.net.InetSocketAddress
import scala.concurrent.duration._

import RawProtocol._
import testkit._

trait PR extends Protocol {
  type Input = String
  type Output = Int
}

class LoadBalancingClientSpec extends ColossusSpec with MockitoSugar{

  implicit val executor = FakeIOSystem.testExecutor

  type C = Sender[PR, Callback]

  trait MockClient extends Sender[PR, Callback] {

  }

  def mockClient(address: InetSocketAddress, customReturn: Option[Try[Int]]): C = {
    val config = ClientConfig(
      address = address,
      name = "/mock",
      requestTimeout = 1.second
    )
    val r = customReturn.getOrElse(Success(address.getPort))
    val c = mock[MockClient]
    when(c.send("hey")).thenReturn(Callback.complete(r))
    c
  }

  def mockClient(port: Int, customReturn: Option[Try[Int]] = None): C = mockClient(new InetSocketAddress("0.0.0.0", port), customReturn)

  def staticClients(l: List[C]): InetSocketAddress => C = {
    var clients = l
    (address) => {
      val next = clients.head
      clients = clients.tail
      next
    }
  }

  val mockGenerator = (address: InetSocketAddress) => mockClient(address, None)

  def addrs(num: Int) = (1 to num).map{i => new InetSocketAddress("0.0.0.0", i)}

  "LoadBalancingClient" must {

    "send two consecutive commands to different clients" in {
      val clients = addrs(3)
      val (probe, worker) = FakeIOSystem.fakeWorkerRef
      val l = new LoadBalancingClient[PR](worker, mockGenerator, initialClients = clients)
      l.send("hey").execute{_ must equal(Success(2))}
      l.send("hey").execute{_ must equal(Success(3))}
      l.send("hey").execute{_ must equal(Success(1))}
      l.send("hey").execute{_ must equal(Success(2))}

    }


    "evenly divide requests among clients" in {
      (1 to 5).foreach{num =>
        val ops = (1 to num).permutations.toList.size //lazy factorial
        val addresses = addrs(num)
        val clients = addresses.map{a => mockClient(a, None)}
        val (probe, worker) = FakeIOSystem.fakeWorkerRef
        val l = new LoadBalancingClient[PR](worker, staticClients(clients.toList), maxTries = 2, initialClients = addresses)
        (1 to ops).foreach{i =>
          l.send("hey").execute()
        }
        clients.foreach{c =>
          verify(c, times(ops / num)).send("hey")
        }
      }
    }

    "properly failover requests" in {
      val bad = mockClient(1, Some(Failure(new Exception("I fucked up :("))))
      val good = mockClient(2, Some(Success(123)))

      val (probe, worker) = FakeIOSystem.fakeWorkerRef
      val l = new LoadBalancingClient[PR](worker, staticClients(List(good, bad)), maxTries = 2, initialClients = addrs(2))
      //sending a bunch of commands ensures both clients are attempted as the first try at least once
      //the test succeeds if no exception is thrown
      (1 to 10).foreach{i =>
        l.send("hey").execute{
          case Success(_) => {}
          case Failure(wat) => throw wat
        }
      }
    }

    "open multiple connections to a single host" in {
      val hosts = (1 to 3).map{i => new InetSocketAddress("0.0.0.0", 1)}
      val clients = (1 to 3).map{i => mockClient(1, Some(Success(i)))}
      val (probe, worker) = FakeIOSystem.fakeWorkerRef
      val l = new LoadBalancingClient[PR](worker, staticClients(clients.toList), maxTries = 2, initialClients = hosts)
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 2
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 3
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
    }

    "removing by address removes all connections" in {
      val hosts = (new InetSocketAddress("1.1.1.1", 5)) :: (1 to 2).map{i => new InetSocketAddress("0.0.0.0", 1)}.toList
      val (probe, worker) = FakeIOSystem.fakeWorkerRef
      val l = new LoadBalancingClient[PR](worker, mockGenerator, maxTries = 2, initialClients = hosts)
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 5
      l.removeClient(new InetSocketAddress("0.0.0.0", 1))
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 5
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 5
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 5
    }

    "update doesn't open duplicate connections when allowDuplicates is false" in {
      val address = new InetSocketAddress("1.1.1.1", 5)
      val (probe, worker) = FakeIOSystem.fakeWorkerRef
      val clients = (1 to 3).map{i => mockClient(1, Some(Success(i)))}
      val l = new LoadBalancingClient[PR](worker, staticClients(clients.toList), maxTries = 2, initialClients = List(address))
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
      l.update(List(address))
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
      CallbackAwait.result(l.send("hey"), 1.second) mustBe 1
    }

    "close removed connection on update" in {
      val fw = FakeIOSystem.fakeWorker

      implicit val w = fw.worker
      val clients = collection.mutable.ArrayBuffer[ServiceClient[Raw]]()
      val generator = (i: InetSocketAddress) => {
        val h = ServiceClient[Raw]("0.0.0.0", i.getPort, 1.second)
        clients += h
        val x = MockConnection.client(h, fw, 1024)
        h.connected(x)
        h
      }
      val l = new LoadBalancingClient[Raw](fw.worker, generator, maxTries = 2, initialClients = addrs(3))

      val removed = clients(0)
      removed.connectionState.isInstanceOf[ConnectionState.Connected] must equal(true)

      val newAddrs = clients.drop(1).map{_.config.address}
      l.update(newAddrs)
      removed.connectionState.isInstanceOf[ConnectionState.ShuttingDown] must equal(true)

    }

  }

  "ServiceClientPool" must {
    val fw = FakeIOSystem.fakeWorker
    def pool() = new ServiceClientPool(
      ClientConfig(address = new InetSocketAddress("0.0.0.0", 1), name = "/foo", requestTimeout = 1.second),
      fw.worker,
      (config, worker) => {
        implicit val w = worker
        val x = MockConnection.client(ServiceClient[Raw](config), fw, 1024)
        x.typedHandler.connected(x)
        x.typedHandler
      }
    )


    "create and get a client" in {
      val p = pool
      val addr = new InetSocketAddress("1.2.3.4", 123)
      p.get(addr) must equal(None)
      val client = p(addr)
      client.config.address must equal(addr)
      p.get(addr) must equal(Some(client))
    }

    "update" in {
      val p = pool
      val addr = new InetSocketAddress("1.2.3.5", 431)
      val addr2 = new InetSocketAddress("1.2.3.5", 432)
      val c = p(addr)
      c.connectionState.isInstanceOf[ConnectionState.Connected] must equal(true)

      p.update(List(addr2))
      c.connectionState.isInstanceOf[ConnectionState.ShuttingDown] must equal(true)
      p.get(addr) must equal(None)
      p.get(addr2).isEmpty must equal(false)
    }
  }


}
