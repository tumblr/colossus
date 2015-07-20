package colossus

import colossus.TestMemcachedServer.WriteParameters
import colossus.core.ServerRef
import colossus.protocols.http.HttpCodes
import colossus.testkit.HttpServiceSpec
import net.liftweb.json.Serialization.write

import scala.concurrent.duration._
/*
Please be aware when running this test, that if you run it on a machine with a memcached server
running on 11211, it will begin to interact with it.  Be mindful if you are running this on a server
which has data you care about.
This test runs by hitting the REST endpoints exposed by teh TestMemcachedServer, which just proxies directly to
a Memcached client, which communicates with memcached
 */
class MemcacheITSpec extends HttpServiceSpec{

  override def service: ServerRef = TestMemcachedServer.start(8889)

  override def requestTimeout: FiniteDuration = 1.second

  "Memcached client" should {
    "add" in {
      val params =  write(WriteParameters("colITAdd", "colITAddValue"))
      testPost("/add", params, HttpCodes.OK, "Stored")
      testGet("/get/colITAdd", "colITAddValue")
      testPost("/add", params, HttpCodes.OK, "NotStored")
    }

    "append" in {
      val params =  write(WriteParameters("colITAppend", "colITAddAppend"))
      val appendParams =  write(WriteParameters("colITAppend", "append"))
      testPost("/append", appendParams, HttpCodes.OK, "NotStored")
      testPost("/add", params, HttpCodes.OK, "Stored")
      testPost("/append", appendParams, HttpCodes.OK, "Stored")
      testGet("/get/colITAppend", "colITAddAppendappend")
    }

    "decr" in {
      val params =  write(WriteParameters("colITDecr", "10"))
      testPost("/add", params, HttpCodes.OK, "Stored")
      testPost("/decr/colITDecr/1", "", HttpCodes.OK, "9")
    }

    "delete" in {
      val params =  write(WriteParameters("colITDel", "colItDelValue"))
      testPost("/add", params, HttpCodes.OK, "Stored")
      testPost("/delete/colITDel", "", HttpCodes.OK)
      testGetCode("/get/colITDel", HttpCodes.NOT_FOUND)
    }

    "incr" in {
      val params =  write(WriteParameters("colITIncr", "10"))
      testPost("/add", params, HttpCodes.OK, "Stored")
      testPost("/incr/colITIncr/1", "", HttpCodes.OK, "11")
    }

    "prepend" in {
      val params =  write(WriteParameters("colITPrepend", "colITAddPrepend"))
      val appendParams =  write(WriteParameters("colITPrepend", "prepend"))
      testPost("/prepend", appendParams, HttpCodes.OK, "NotStored")
      testPost("/add", params, HttpCodes.OK, "Stored")
      testPost("/prepend", appendParams, HttpCodes.OK, "Stored")
      testGet("/get/colITPrepend", "prependcolITAddPrepend")
    }

    "replace" in {
      val replaceParams =  write(WriteParameters("colITReplace", "replaced!"))
      val addParams =  write(WriteParameters("colITReplace", "colItReplaceValue"))
      testPost("/replace", replaceParams, HttpCodes.OK, "NotStored")
      testPost("/add", addParams, HttpCodes.OK, "Stored")
      testPost("/replace", replaceParams, HttpCodes.OK, "Stored")
      testGet("/get/colITReplace", "replaced!")
    }

    "set" in {
      val params =  write(WriteParameters("colITSet", "colITSetValue"))
      testPost("/set", params, HttpCodes.OK, "Stored")
      testGet("/get/colITSet", "colITSetValue")
      val params2 =  write(WriteParameters("colITSet", "colITSetValueAgain"))
      testPost("/set", params2, HttpCodes.OK, "Stored")
      testGet("/get/colITSet", "colITSetValueAgain")
    }

    "touch" in {
      val params =  write(WriteParameters("colITTouch", "colITTouchValue"))
      testPost("/touch/colITTouch/100", params, HttpCodes.NOT_FOUND)
      testPost("/set", params, HttpCodes.OK, "Stored")
      testPost("/touch/colITTouch/100", params, HttpCodes.OK, "Touched")
    }
  }
}
