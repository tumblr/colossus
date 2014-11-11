package colossus


import org.scalatest._

import akka.util.ByteString

import colossus.protocols.redis._

class RedisMonitorClientSuite extends WordSpec with MustMatchers {

  "Monitor parseLine" must {
    "parse basic" in {
      val line = ByteString("""1400597974.077549 [0 127.0.0.1:52916] "set" "foo" "bar"""")
      RedisMonitorClient.parseLine(line) must equal(List(ByteString("set"), ByteString("foo"), ByteString("bar")))
    }

    "parse escape sequences" in {
      val line = ByteString("""1400597974.077549 [0 127.0.0.1:52916] "set" "f\"oo" "b\tar\n"""")
      RedisMonitorClient.parseLine(line) must equal(List(ByteString("set"), ByteString("""f"oo"""), ByteString("b\tar\n")))

    }
    "preserve non-escape sequences" in {
      val line = ByteString("""1400597974.077549 [0 127.0.0.1:52916] "set" "foo" "b\gr"""")
      RedisMonitorClient.parseLine(line) must equal(List(ByteString("set"), ByteString("foo"), ByteString("""b\gr""")))

    }

    "parse unicode" in {
      val line = ByteString("""1400599404.843562 [0 127.0.0.1:53085] "set" "foo" " (\xe2\x95\xaf\xc2\xb0\xe2\x96\xa1\xc2\xb0\xef\xbc\x89\xe2\x95\xaf\xef\xb8\xb5 \xe2\x94\xbb\xe2\x94\x81\xe2\x94\xbb"""")
      RedisMonitorClient.parseLine(line) must equal(List(ByteString("set"), ByteString("foo"), ByteString(""" (╯°□°）╯︵ ┻━┻""")))
    }


  }


}


