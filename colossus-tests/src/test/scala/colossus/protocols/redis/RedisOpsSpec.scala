package colossus.protocols.redis

import akka.util.ByteString
import colossus.service._
import org.scalatest._

class RedisOpsSpec extends FlatSpec with ShouldMatchers with RedisClient[Callback]{

  override def client: Sender[Redis, Callback] = new BasicLiftedClient(null)
  override implicit val async: Async[Callback] = CallbackAsync

  it should "convert Double to a string properly" in {
    bs(1234) should equal (ByteString("1234"))
    bs(1234.5) should equal (ByteString("1234.5"))
    bs(123456789) should equal (ByteString("123456789"))
  }
}
