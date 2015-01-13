package colossus.examples

import colossus.IOSystem
import colossus.core.{ServerRef}
import colossus.service._
import colossus.protocols.telnet._
import Callback.Implicits._


object TelnetExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {

    Service.serve[Telnet]("telnet-test", port) { context => 
      context.handle { connection => 
        connection.become {
          case TelnetCommand("exit" :: Nil) => {
            connection.gracefulDisconnect()
            TelnetReply("Bye!")
          }
          case TelnetCommand(List("say", arg)) => TelnetReply(arg)
          case other => TelnetReply(other.toString)
        }
      }
    }
  }
}

