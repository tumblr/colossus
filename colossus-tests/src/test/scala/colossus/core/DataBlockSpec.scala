package colossus
package core

import testkit._
import akka.util.{ByteString, ByteStringBuilder}

class DataBlockSpec extends ColossusSpec {

  "DataBlock" must {

    "respect equality" in {
      DataBlock("123456") == DataBlock("123456") must equal(true)
    }

    "generate same hashcode for same data" in {
      DataBlock("123456abcdef").hashCode must equal(DataBlock("123456abcdef").hashCode)
    }

    "take" in {
      DataBlock("12345").take(3) must equal(DataBlock("123"))
    }

    "drop" in {
      DataBlock("12345").drop(3) must equal(DataBlock("45"))
    }

  }
}



