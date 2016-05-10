package colossus.core

import colossus.parsing.{InvalidDataSizeException, DataSize}
import org.scalatest.{MustMatchers, WordSpec}

class DataSizeSpec extends WordSpec with MustMatchers{

  "Datasize" must {
    "be created from a string" in {

      val sizes : Map[String, DataSize] = Map(
          "1 B"->DataSize(1),
          "1 KB"-> DataSize(1024),
          "1 MB" -> DataSize(1024*1024),
          "2 b" -> DataSize(2),
          "2 Kb" -> DataSize(2 * 1024),
          "2 mB" -> DataSize(1024*1024*2))

      sizes.foreach{ case (k, v) => DataSize(k) mustBe v }


    }
    "throw an InvalidDataSize Exception if the creation string is invalid" in {
      val badSizes = Seq("-1 B", "1", "KB")
      badSizes foreach { x =>
        intercept[InvalidDataSizeException]{
          DataSize(x)
        }
      }
    }

    "implicits should work" in {
      import colossus.parsing.DataSize._
      1L.MB mustBe DataSize("1 MB")
      1L.KB mustBe DataSize("1 KB")
    }
  }
}
