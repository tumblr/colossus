package colossus

import colossus.core.ServerRef
import colossus.protocols.http.HttpCodes
import colossus.testkit.HttpServiceSpec

import scala.concurrent.duration.{FiniteDuration, _}
/*
Please be aware when running this test, that if you run it on a machine with a redis server
running on 6379, it will begin to interact with it.  Be mindful if you are running this on a server
which has data you care about.
This test runs by hitting the REST endpoints exposed by teh TestRedisServer, which just proxies directly to
a Redis client, which communicates with redis
 */
class RedisITSpec extends HttpServiceSpec {
  override def service: ServerRef = TestRedisServer.start(8888)

  override def requestTimeout: FiniteDuration = 1.seconds

  "Redis Client" should {

    "del" in {
      testPost("/set/colITDel/colITDel", "", HttpCodes.OK)
      testPost("/del/colITDel", "1", HttpCodes.OK)
    }

    "existence" in {
      testGet("/exists/colITEx", "false")
      testPost("/set/colITEx/colITEx", "", HttpCodes.OK)
      testGet("/exists/colITEx", "true")
    }

    "set && get" in {
      testPost("/set/colITSet/colITSet", "", HttpCodes.OK)
      testGet("/get/colITSet", "colITSet")
    }

    "setnx" in {
      //returns true if set properly
      testPost("/setnx/colITSetnx/colITSetnx", "", HttpCodes.OK, "true")
      testGet("/get/colITSetnx", "colITSetnx")
      //returns false if setnx did not set anything
      testPost("/setnx/colITSetnx/colITSetnx", "", HttpCodes.OK, "false")
    }

    "setex && ttl" in {
      //returns true if set properly
      testPost("/setex/colITSetex/colITSetex/500", "", HttpCodes.OK)
      testGet("/get/colITSetex", "colITSetex")
      testGet("/ttl/colITSetex", "500")
    }

    "strlen" in {
      testPost("/set/colITStrlen/colITStrlen", "", HttpCodes.OK)
      testGet("/strlen/colITStrlen", "11")
    }
  }
}