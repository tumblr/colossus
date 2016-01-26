package colossus.examples

import colossus.IOSystem
import colossus.core.{Server, ServerRef}
import colossus.service._
import colossus.protocols.telnet._
import Callback.Implicits._


object TelnetExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {

    Server.start("telnet-test", port) { context => 
      context onConnect { connection => 
        connection accept new Service[Telnet] {
          def handle = {
            case TelnetCommand("exit" :: Nil) => {
              disconnect()
              TelnetReply("Bye!")
            }
            case TelnetCommand(List("say", arg)) => TelnetReply(arg)
            case other => TelnetReply(other.toString)
          }
        }
      }
    }
  }
}

