package colossus.examples


import colossus._
import colossus.core._
import colossus.service._
import colossus.protocols.http._
import colossus.protocols.websocket._

import akka.actor._
import akka.util.ByteString

/*
class PrimeGenerator extends Actor {

  var lastPrime = 1

  def receive = {
    case c: Context => context.become(sending(c))
  }

  def sending(c: Context): Receive = {
    case Next => {
      var nextPrime = lastPrime
      var prime = false
      while (!prime) {
        nextPrime += 1
        var n = 1
        var ok = true
        while (n < nextPrime && ok) {
          n += 1
          if (nextPrime % n == 0) {
            ok = false
          }
        }
        prime = ok
      }
      c ! nextPrime
    }
  }
}
 */         
    


object WebsocketExample {

  def start(port: Int)(implicit io: IOSystem) = {
    Server.basic("websocket", port){ new Service[Http](_) {
      override def shutdown() {
        println("shutting down")
        super.shutdown()
      }
      def handle = {
        case WebsocketUpgradeRequest(resp) => {
          become(new WebsocketHandler(_){
            println("starting websocket")           

            override def preStart() {
              send(ByteString("HELLO THERE!"))
            }

            def handle = {
              case bytes => {
                println("Got bytes " + bytes.utf8String)
                send(ByteString(s"got " + bytes.utf8String))
              }
            }
          })
          Callback.successful(resp)
        }
      }
    }}
  }
}
