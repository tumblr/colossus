package colossus.examples


import colossus._
import colossus.core._
import colossus.service._
import colossus.protocols.http._
import colossus.protocols.websocket._

import akka.actor._
import akka.util.ByteString

import scala.concurrent.duration._

import Http.defaults._

class PrimeGenerator extends Actor {

  var lastPrime = 1

  def receive = {
    case Next => sender() ! nextPrime
  }

  def nextPrime = {
    var nextPrime = lastPrime
    var prime = false
    while (!prime) {
      nextPrime += 1
      var n = 1
      var ok = true
      while (n < nextPrime - 1 && ok) {
        n += 1
        if (nextPrime % n == 0) {
          ok = false
        }
      }
      prime = ok
    }
    lastPrime = nextPrime
    nextPrime
  }
}

case object Next
    


object WebsocketExample {

  def start(port: Int)(implicit io: IOSystem) = {
    
    val generator = io.actorSystem.actorOf(Props[PrimeGenerator])

    Server.basic("websocket", port){ new Service[Http](_) {
      def handle = {
        case UpgradeRequest(resp) => {
          become(new WebsocketHandler(_) with ProxyActor {

            private var sending = false

            override def preStart() {
              send(ByteString("HELLO THERE!"))
            }

            override def shutdown() {
              send(ByteString("goodbye!"))
              super.shutdown()
            }

            def handle = {
              case bytes => bytes.utf8String.toUpperCase match {
                case "START" => {
                  sending = true
                  generator ! Next
                }
                case "STOP" => {
                  sending = false
                }
                case "EXIT" => {
                  disconnect()
                }
              }
            }

            def receive = {
              case prime: Integer => {
                send(ByteString(s"PRIME: $prime"))
                if(sending) {
                  import io.actorSystem.dispatcher
                  io.actorSystem.scheduler.scheduleOnce(100.milliseconds, generator , Next)
                }
              }
            }

          })
          Callback.successful(resp)
        }
      }
    }}
  }
}
