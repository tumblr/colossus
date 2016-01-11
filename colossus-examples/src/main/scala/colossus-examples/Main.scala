package colossus.examples

import colossus._
import akka.actor._
import java.net.InetSocketAddress



object Main extends App {

  println(""" _______  _______  _        _______  _______  _______           _______ """)
  println("""(  ____ \(  ___  )( \      (  ___  )(  ____ \(  ____ \|\     /|(  ____ \""")
  println("""| (    \/| (   ) || (      | (   ) || (    \/| (    \/| )   ( || (    \/""")
  println("""| |      | |   | || |      | |   | || (_____ | (_____ | |   | || (_____ """)
  println("""| |      | |   | || |      | |   | |(_____  )(_____  )| |   | |(_____  )""")
  println("""| |      | |   | || |      | |   | |      ) |      ) || |   | |      ) |""")
  println("""| (____/\| (___) || (____/\| (___) |/\____) |/\____) || (___) |/\____) |""")
  println("""(_______/(_______)(_______/(_______)\_______)\_______)(_______)\_______)""")

  implicit val actorSystem = ActorSystem("COLOSSUS")

  implicit val ioSystem = IOSystem()

  //the simplest example, an echo server over telnet
  val telnetServer = TelnetExample.start(9000)

  //http service which communicates with a key/value store over the redis protocol
  val httpServer = HttpExample.start(9001, new InetSocketAddress("localhost", 9002))

  //and here's the key/value store itself
  val keyvalServer = KeyValExample.start(9002)

  //an echo server built only on the core layer
  val echoServer = EchoExample.start(9003)

  //chat server using the controller layer
  val chatServer = ChatExample.start(9005)

  val benchmarkServer = BenchmarkService.start(9007)

  {
    import colossus.metrics._
    import colossus.protocols.http._
    import colossus.service._

    val c = new CollectionMap
    Service.serve[Http]("foo", 11111){ctx =>
      ctx.handle{con =>
        con.become {
          case any => {
            (0 to 5).foreach{i => c.increment(Map("foo" -> "bar")) }
            Callback.successful(any.ok("done"))
          }
        }
      }
    }
    Service.serve[Http]("bar", 11112){ctx =>
      val r = ctx.worker.metrics.getOrAdd(Rate("bleh"))
      ctx.handle{con =>
        con.become {
          case any => {
            r.hit(Map("foo" -> "bar"))
            Callback.successful(any.ok("done"))
          }
        }
      }
    }
  }


            

}
