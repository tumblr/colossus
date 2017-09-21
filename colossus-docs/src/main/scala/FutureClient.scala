import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http.{Http, HttpRequest}

object FutureClient extends App {

  // #future_client
  implicit val actorSystem      = ActorSystem()
  implicit val executionContext = actorSystem.dispatcher
  implicit val ioSystem         = IOSystem()

  val futureClient = Http.futureClient("example.org", 80)

  val request = HttpRequest.get("/")

  val futureResponse = futureClient.send(request)
  // #future_client

  futureResponse.foreach { response =>
    println(response)
  }
}
