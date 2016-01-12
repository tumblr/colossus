package colossus
//where should this go...

import service._

import metrics._
import MetricAddress._
import akka.actor._
import akka.util.Timeout
import scala.concurrent.duration._

import protocols.http._

import java.net.InetSocketAddress
import net.liftweb.json._

/*

class JsonMetricSenderActor(io: IOSystem, host: String, port: Int, path: String) extends Actor with ActorLogging {
  import MetricSender._

  val config = ClientConfig(
    address = new InetSocketAddress(host, port),
    name = "json-sender",
    requestTimeout = 750.milliseconds
  )
  implicit val _io = io
  implicit val timeout = Timeout(1.seconds)
  import context.dispatcher

  val client = AsyncServiceClient(config, new HttpClientCodec)

  def receive = {
    case Send(map, gtags, timestamp) => {
      val body = compact(render(map.addTags(gtags).toJson))
      val request = HttpRequest(HttpMethod.Post, path, Some(body))
      client.send(request).map{response => response.head.code match {
        case HttpCodes.OK => {}
        case other => log.warning(s"got error from aggregator: $response")
      }}      
    }
  }

}

case class JsonMetricSender(host: String, port: Int, path: String, sys: IOSystem = IOSystem("json-sender")) extends MetricSender {
  def name = "json"
  def props = Props(classOf[JsonMetricSenderActor], sys, host, port, path)
}


*/
