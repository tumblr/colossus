package colossus.util.bson

import colossus.util.bson.BsonDsl._
import colossus.util.bson.Implicits._
import colossus.util.bson.element.BsonObjectId
import colossus.util.bson.reader.BsonDocumentReader
import org.joda.time.DateTime
import org.scalatest.{Matchers, WordSpec}

class BsonSpec extends WordSpec with Matchers {

  "Bson" must {

    "encode and decode BsonDocument" in {
      val expected = document(
        "_id" := BsonObjectId.generate,
        "name" := "jack",
        "age" := 18,
        "months" := array(1, 2, 3),
        "details" := document(
          "salary" := 455.5,
          "inventory" := array("a", 3.5, 1L, true),
          "birthday" := new DateTime(1987, 3, 5, 0, 0)
        )
      )

      val encoded = expected.encode()
      val actual = BsonDocumentReader(encoded).read
      actual should be(Some(expected))
    }
  }
}
