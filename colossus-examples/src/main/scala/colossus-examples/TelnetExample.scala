package colossus.examples

import colossus.IOSystem
import colossus.core.{ServerRef}
import colossus.service._
import colossus.protocols.telnet._


object TelnetExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {

    Service.become[Telnet]("telnet-test", port) {
      case TelnetCommand("exit" :: Nil) => TelnetReply("Bye!").onWrite(OnWriteAction.Disconnect)
      case TelnetCommand(List("say", arg)) => TelnetReply(arg)
      case other => TelnetReply(other.toString)
    }

  }
}

