package colossus

import colossus.TestMemcachedServer.{GetReplies, GetReply, WriteParameters}
import colossus.core.ServerRef
import colossus.protocols.http.HttpCodes
import colossus.protocols.memcache.MemcacheReply.Value
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
      testGetJson("/get/colITAdd",  GetReply("colITAdd", "colITAddValue", 0))
      testPost("/add", params, HttpCodes.OK, "NotStored")
    }

    "append" in {
      val params =  write(WriteParameters("colITAppend", "colITAddAppend"))
      val appendParams =  write(WriteParameters("colITAppend", "append"))
      testPost("/append", appendParams, HttpCodes.OK, "NotStored")
      testPost("/add", params, HttpCodes.OK, "Stored")
      testPost("/append", appendParams, HttpCodes.OK, "Stored")
      testGetJson("/get/colITAppend",  GetReply("colITAppend", "colITAddAppendappend", 0))
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
    "get multiple keys" in {
      val paramsA =  write(WriteParameters("colITGetA", "colITGetAValue"))
      val paramsB =  write(WriteParameters("colITGetB", "colITGetBValue"))
      testPost("/set", paramsA, HttpCodes.OK, "Stored")
      testPost("/set", paramsB, HttpCodes.OK, "Stored")
      testGetJson("/get/colITGetA_colITGetB",  GetReplies(Seq(GetReply("colITGetA", "colITGetAValue", 0), GetReply("colITGetB", "colITGetBValue", 0))))
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
      testGetJson("/get/colITPrepend",  GetReply("colITPrepend", "prependcolITAddPrepend", 0))
    }

    "replace" in {
      val replaceParams =  write(WriteParameters("colITReplace", "replaced!"))
      val addParams =  write(WriteParameters("colITReplace", "colItReplaceValue"))
      testPost("/replace", replaceParams, HttpCodes.OK, "NotStored")
      testPost("/add", addParams, HttpCodes.OK, "Stored")
      testPost("/replace", replaceParams, HttpCodes.OK, "Stored")
      testGetJson("/get/colITReplace",  GetReply("colITReplace", "replaced!", 0))
    }

    "set" in {
      val params =  write(WriteParameters("colITSet", "colITSetValue", 123))
      testPost("/set", params, HttpCodes.OK, "Stored")
      testGetJson("/get/colITSet",  GetReply("colITSet", "colITSetValue", 123))
      val params2 =  write(WriteParameters("colITSet", "colITSetValueAgain", 123))
      testPost("/set", params2, HttpCodes.OK, "Stored")
      testGetJson("/get/colITSet",  GetReply("colITSet", "colITSetValueAgain", 123))
    }

    "touch" in {
      val params =  write(WriteParameters("colITTouch", "colITTouchValue"))
      testPost("/touch/colITTouch/100", params, HttpCodes.NOT_FOUND)
      testPost("/set", params, HttpCodes.OK, "Stored")
      testPost("/touch/colITTouch/100", params, HttpCodes.OK, "Touched")
    }
  }
}
