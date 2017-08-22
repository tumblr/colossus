package colossus.examples

import colossus._
import colossus.core._
import colossus.protocols.websocket._
import colossus.streaming.PushResult
import subprotocols.rawstring._

import akka.actor._

import scala.concurrent.duration._

class PrimeGenerator extends Actor {

  var lastPrime = 1

  def receive = {
    case Next => sender() ! nextPrime
  }

  def nextPrime = {
    var nextPrime = lastPrime
    var prime     = false
    while (!prime) {
      nextPrime += 1
      var n  = 1
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

    WebsocketServer.start[RawString]("websocket", port) { worker =>
      new WebsocketInitializer[RawString](worker) {

        def provideCodec() = new RawStringCodec

        def onConnect =
          ctx =>
            new WebsocketServerHandler[RawString](ctx) with ProxyActor {
              private var sending = false

              def shutdownRequest() {
                upstream.connection.disconnect()
              }

              override def onConnected() {
                send("HELLO THERE!")
              }

              override def onShutdown() {
                send("goodbye!")
              }

              def handle = {
                case "START" => {
                  sending = true
                  generator ! Next
                }
                case "STOP" => {
                  sending = false
                }
                case "LARGE" => {
                  send((0 to 1000).mkString)
                }
                case "MANY" => {
                  //send one message per event loop iteration
                  def next(i: Int) {
                    if (i > 0) send(i.toString) match {
                      case PushResult.Ok           => next(i - 1)
                      case PushResult.Full(signal) => signal.notify { next(i - 1) }
                      case _                       => {}
                    }
                  }
                  next(1000)
                }
                case "EXIT" => {
                  //uhhh
                  upstream.connection.disconnect()
                }
                case other => {
                  send(s"unknown command: $other")
                }
              }

              def handleError(reason: Throwable) {}

              def receive = {
                case prime: Integer => {
                  send(s"PRIME: $prime")
                  if (sending) {
                    import io.actorSystem.dispatcher
                    io.actorSystem.scheduler.scheduleOnce(100.milliseconds, generator, Next)
                  }
                }
              }

          }

      }
    }

  }
}
