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
import testkit.MockConnection

trait PR extends Protocol {
  type Input = String
  type Output = Int
}

class LoadBalancingClientSpec extends ColossusSpec with MockitoSugar{

  type C = ServiceClient[PR]
  
  def mockClient(address: InetSocketAddress, customReturn: Option[Try[Int]]): C = {
    val config = ClientConfig(
      address = address,
      name = "/mock",
      requestTimeout = 1.second
    )
    val r = customReturn.getOrElse(Success(address.getPort))
    val c = mock[ServiceClient[PR]]
    when(c.send("hey")).thenReturn(Callback.complete(r))
    when(c.config).thenReturn(config)
    when(c.connectionState).thenReturn(core.ConnectionState.Connected(mock[core.WriteEndpoint]))
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
      l.send("hey").execute{_ must equal(Success(1))}
      l.send("hey").execute{_ must equal(Success(3))}
      l.send("hey").execute{_ must equal(Success(2))}
      l.send("hey").execute{_ must equal(Success(1))}

    }


    "evenly divide requests among clients" in {
      (1 to 5).foreach{num => 
        val ops = (1 to num).permutations.toList.size //lazy factorial
        val clients = addrs(num)
        val (probe, worker) = FakeIOSystem.fakeWorkerRef
        val l = new LoadBalancingClient[PR](worker, mockGenerator, maxTries = 2, initialClients = clients)
        (1 to ops).foreach{i => 
          l.send("hey").execute()
        }
        l.currentClients.foreach{case (i,c) =>
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

    "close removed connection on update" ignore {
     /* TODO this won't work until connectionState is added to Sender
      val fw = FakeIOSystem.fakeWorker

      implicit val w = fw.worker
      val generator = (i: InetSocketAddress) => {
        val h = ServiceClient[Raw]("0.0.0.0", i.getPort, 1.second)
        val x = MockConnection.client(h, fw, 1024)
        h.connected(x)
        h
      }
      val l = new LoadBalancingClient[Raw](fw.worker, generator, maxTries = 2, initialClients = addrs(3))
      val clients = l.currentClients

      val removed = clients(0)
      removed.connectionState.isInstanceOf[ConnectionState.Connected] must equal(true)

      val newAddrs = clients.drop(1).map{_.config.address}
      l.update(newAddrs)
      removed.connectionState.isInstanceOf[ConnectionState.ShuttingDown] must equal(true)
      */

    }
      



  }


}
