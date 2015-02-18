package colossus
package service

import core.WorkerCommand
import colossus.testkit.{ColossusSpec, FakeIOSystem}
import org.scalatest.{WordSpec, MustMatchers}
import org.scalatest.mock.MockitoSugar

import org.mockito.Mockito._

import scala.concurrent.{ExecutionContext, Future, Promise}
import ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import java.net.InetSocketAddress
import scala.concurrent.duration._



class LoadBalancingClientSpec extends ColossusSpec with MockitoSugar{

  type C = ServiceClient[String,Int]
  
  def mockClient(address: InetSocketAddress, customReturn: Option[Try[Int]]): C = {
    val config = ClientConfig(
      address = address,
      name = "/mock",
      requestTimeout = 1.second
    )
    val r = customReturn.getOrElse(Success(address.getPort))
    val c = mock[ServiceClient[String, Int]]
    when(c.send("hey")).thenReturn(Callback.complete(r))
    when(c.config).thenReturn(config)
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
      val l = new LoadBalancingClient[String,Int](worker, mockGenerator, initialClients = clients)
      l.send("hey").execute{_ must equal(Success(2))}
      l.send("hey").execute{_ must equal(Success(3))}
      l.send("hey").execute{_ must equal(Success(1))}
      l.send("hey").execute{_ must equal(Success(2))}

    }


    "evenly divide requests among clients" in {
      (1 to 5).foreach{num => 
        val ops = (1 to num).permutations.toList.size //lazy factorial
        val clients = addrs(num)
        val (probe, worker) = FakeIOSystem.fakeWorkerRef
        val l = new LoadBalancingClient[String,Int](worker, mockGenerator, maxTries = 2, initialClients = clients)
        (1 to ops).foreach{i => 
          l.send("hey").execute()
        }
        l.currentClients.foreach{c =>
          verify(c, times(ops / num)).send("hey")
        }
      }
    }

    "properly failover requests" in {
      val bad = mockClient(1, Some(Failure(new Exception("I fucked up :("))))
      val good = mockClient(2, Some(Success(123)))

      val (probe, worker) = FakeIOSystem.fakeWorkerRef
      val l = new LoadBalancingClient[String,Int](worker, staticClients(List(good, bad)), maxTries = 2, initialClients = addrs(2))
      //sending a bunch of commands ensures both clients are attempted as the first try at least once
      //the test succeeds if no exception is thrown
      (1 to 10).foreach{i => 
        l.send("hey").execute{
          case Success(_) => {}
          case Failure(wat) => throw wat 
        }
      }
    }

    "close removed connection on update" in {
      val (probe, worker) = FakeIOSystem.fakeWorkerRef
      val l = new LoadBalancingClient[String, Int](worker, mockGenerator, maxTries = 2, initialClients = addrs(3))
      val clients = l.currentClients

      val removed = clients(0)

      val newAddrs = clients.drop(1).map{_.config.address}
      l.update(newAddrs)

      verify(removed).gracefulDisconnect()
    }
      



  }


}
