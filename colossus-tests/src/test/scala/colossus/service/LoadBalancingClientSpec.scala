package colossus
package service

import colossus.testkit.{ColossusSpec, FakeIOSystem}
import org.scalamock.scalatest.MockFactory

import scala.util.{Try, Success, Failure}
import java.net.InetSocketAddress
import scala.concurrent.duration._

import testkit._

trait PR extends Protocol {
  type Request = String
  type Response = Int
}

class LoadBalancingClientSpec extends ColossusSpec with MockFactory{
  
  type C = Sender[PR, Callback]
  
  implicit val executor = FakeIOSystem.testExecutor

  trait MockClient extends Sender[PR, Callback] {

  }

  def mockClient(address: InetSocketAddress, customReturn: Option[Try[Int]]): C = {
    val r = customReturn.getOrElse(Success(address.getPort))
    val c = stub[C]
    (c.send _).when("hey").returns(Callback.complete(r))
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
      def fakeSender() = new Sender[PR, Callback] {
        var numCalled = 0
        def send(s: String): Callback[Int] = {
          numCalled += 1
          Callback.successful(numCalled)
        }
        def disconnect() {}
      }
      (1 to 5).foreach{num => 
        val ops = (1 to num).permutations.toList.size //lazy factorial
        val addresses = addrs(num)
        val clients = addresses.map{a => fakeSender()}
        val (probe, worker) = FakeIOSystem.fakeWorkerRef
        val l = new LoadBalancingClient[PR](worker,staticClients(clients.toList), maxTries = 2, initialClients = addresses)
        (1 to ops).foreach{i => 
          l.send("hey").execute()
        }
        clients.foreach{c =>
          // +1 is cause we're calling it again just to get the value
          CallbackAwait.result(c.send("hey"), 1.second) mustBe ((ops / num) + 1)
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
      val clients = collection.mutable.ArrayBuffer[(C, InetSocketAddress)]()
      val generator = (i: InetSocketAddress) => {
        val h = mockClient(i.getPort)
        clients += (h -> i)
        h
        /*
        val h = Raw.clientFactory("0.0.0.0", i.getPort, 1.second)
        val x = MockConnection.client(h, fw, 1024)
        h.connected(x)
        h
        */
      }
      val l = new LoadBalancingClient[PR](fw.worker, generator, maxTries = 2, initialClients = addrs(3))

      val (removed, removedAddress) = clients(0)

      val newAddrs = clients.drop(1).map{_._2}
      l.update(newAddrs)
      (removed.disconnect _).verify()

    }
      
  }

  "ServiceClientPool" must {
    val fw = FakeIOSystem.fakeWorker
    def pool() = new ServiceClientPool(
      ClientConfig(address = new InetSocketAddress("0.0.0.0", 1), name = "/foo", requestTimeout = 1.second),
      fw.worker,
      (config, worker) => {
        mockClient(config.address.getPort)
      }
    )
    
    
    "create and get a client" in {
      val p = pool
      val addr = new InetSocketAddress("1.2.3.4", 123)
      p.get(addr) must equal(None)
      val client = p(addr)
      CallbackAwait.result(client.send("hey"), 1.second) must equal(123)
      p.get(addr) must equal(Some(client))
    }

    "update" in {
      val p = pool
      val addr = new InetSocketAddress("1.2.3.5", 431)
      val addr2 = new InetSocketAddress("1.2.3.5", 432)
      val c = p(addr)

      p.update(List(addr2))
      (c.disconnect _).verify()
      p.get(addr) must equal(None)
      p.get(addr2).isEmpty must equal(false)
    }
  }


}

