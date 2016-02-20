package colossus
package protocols.websocket

import core.DataBuffer

import org.scalatest._

import akka.util.ByteString

import scala.util.Success

class WebsocketSpec extends WordSpec with MustMatchers{

  "ProcessKey" must {
    "correctly translate key from RFC" in {
      WebsocketUpgradeRequest.processKey("dGhlIHNhbXBsZSBub25jZQ==") must equal("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
    }
  }
}

