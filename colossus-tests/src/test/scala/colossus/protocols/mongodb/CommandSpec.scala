package colossus.protocols.mongodb

import java.nio.ByteOrder

import akka.util.ByteString
import colossus.protocols.mongodb.command._
import colossus.util.bson.BsonDsl._
import colossus.util.bson.Implicits._
import org.scalatest.{FlatSpec, Matchers}

class CommandSpec extends FlatSpec with Matchers {

  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  val databaseName = "colossus"
  val collectionName = "redis"

  "Command" should "construct correct Count" in {
    val query = document("age" := 18)

    val actual = Count(databaseName, collectionName, Some(query))

    val expected = {
      val body = ByteString.newBuilder
        .putInt(0)
        .putBytes((databaseName + ".$cmd").getBytes("utf-8"))
        .putByte(0)
        .putInt(0) // numberToSkip
        .putInt(1) // numberToReturn
        .append((("count" := collectionName) + ("query" := query)).encode())
        .result()

      ByteString.newBuilder
        .putInt(body.size + 16)
        .putInt(actual.requestID)
        .putInt(0)
        .putInt(2004)
        .append(body)
        .result()
    }

    actual.encode() should be(expected)
  }

  it should "construct correct Delete" in {
    val deletes = Seq(document("age" := 18))

    val actual = Delete(databaseName, collectionName, deletes)

    val expected = {
      val body = ByteString.newBuilder
        .putInt(0)
        .putBytes((databaseName + ".$cmd").getBytes("utf-8"))
        .putByte(0)
        .putInt(0) // numberToSkip
        .putInt(1) // numberToReturn
        .append((("delete" := collectionName) + ("deletes" := array(deletes: _*))).encode())
        .result()

      ByteString.newBuilder
        .putInt(body.size + 16)
        .putInt(actual.requestID)
        .putInt(0)
        .putInt(2004)
        .append(body)
        .result()
    }

    actual.encode() should be(expected)
  }

  it should "construct correct FindAndModify" in {
    val query = Some(document("age" := 18))

    val actual = FindAndModify(databaseName, collectionName, query, removeOrUpdate = Left(true))

    val expected = {
      val body = ByteString.newBuilder
        .putInt(0)
        .putBytes((databaseName + ".$cmd").getBytes("utf-8"))
        .putByte(0)
        .putInt(0) // numberToSkip
        .putInt(1) // numberToReturn
        .append((
        ("findAndModify" := collectionName) +
          ("query" := query) +
          ("remove" := true) +
          ("new" := false) +
          ("upsert" := false))
        .encode())
        .result()

      ByteString.newBuilder
        .putInt(body.size + 16)
        .putInt(actual.requestID)
        .putInt(0)
        .putInt(2004)
        .append(body)
        .result()
    }

    actual.encode() should be(expected)
  }

  it should "construct correct GetLastError" in {
    val actual = GetLastError(databaseName)

    val expected = {
      val body = ByteString.newBuilder
        .putInt(0)
        .putBytes((databaseName + ".$cmd").getBytes("utf-8"))
        .putByte(0)
        .putInt(0) // numberToSkip
        .putInt(1) // numberToReturn
        .append(document("getLastError" := 1).encode())
        .result()

      ByteString.newBuilder
        .putInt(body.size + 16)
        .putInt(actual.requestID)
        .putInt(0)
        .putInt(2004)
        .append(body)
        .result()
    }

    actual.encode() should be(expected)
  }

  it should "construct correct Insert" in {
    val documents = Seq(document("age" := 18))

    val actual = Insert(databaseName, collectionName, documents)

    val expected = {
      val body = ByteString.newBuilder
        .putInt(0)
        .putBytes((databaseName + ".$cmd").getBytes("utf-8"))
        .putByte(0)
        .putInt(0) // numberToSkip
        .putInt(1) // numberToReturn
        .append((("insert" := collectionName) + ("documents" := array(documents: _*)) + ("ordered" := true)).encode())
        .result()

      ByteString.newBuilder
        .putInt(body.size + 16)
        .putInt(actual.requestID)
        .putInt(0)
        .putInt(2004)
        .append(body)
        .result()
    }

    actual.encode() should be(expected)
  }

}
