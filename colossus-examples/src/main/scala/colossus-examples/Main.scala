package colossus.examples

import colossus._
import akka.actor._
import java.net.InetSocketAddress



object Main extends App {

  override def main(args: Array[String]) {
    println(""" _______  _______  _        _______  _______  _______           _______ """)
    println("""(  ____ \(  ___  )( \      (  ___  )(  ____ \(  ____ \|\     /|(  ____ \""")
    println("""| (    \/| (   ) || (      | (   ) || (    \/| (    \/| )   ( || (    \/""")
    println("""| |      | |   | || |      | |   | || (_____ | (_____ | |   | || (_____ """)
    println("""| |      | |   | || |      | |   | |(_____  )(_____  )| |   | |(_____  )""")
    println("""| |      | |   | || |      | |   | |      ) |      ) || |   | |      ) |""")
    println("""| (____/\| (___) || (____/\| (___) |/\____) |/\____) || (___) |/\____) |""")
    println("""(_______/(_______)(_______/(_______)\_______)\_______)(_______)\_______)""")

    implicit val actorSystem = ActorSystem("COLOSSUS")

    implicit val ioSystem = IOSystem("examples", numWorkers = Some(1))

    //the simplest example, an echo server over telnet
    val telnetServer = TelnetExample.start(9000)

    //http service which communicates with a key/value store over the redis protocol
    val httpServer = HttpExample.start(9001, new InetSocketAddress("localhost", 9002))

    //and here's the key/value store itself
    val keyvalServer = KeyValExample.start(9002)

    //an echo server built only on the core layer
    val echoServer = EchoExample.start(9003)

    //a simple firehose using the Actor API
    val streamServer = StreamExample.start(9004)

  }

}
