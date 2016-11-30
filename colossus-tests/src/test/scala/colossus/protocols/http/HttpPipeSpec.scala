package colossus
package protocols.http

import core._
import controller.Pipe
import controller.PushResult
import testkit.PipeFoldTester

import org.scalatest._

import akka.util.ByteString

import service.Callback
import scala.util.{Try, Success, Failure}




class HttpPipeSuite extends WordSpec with MustMatchers{

  val chunkedBody = ByteString("3\r\nfoo\r\nb\r\n12345678901\r\n0\r\n\r\n")
  val badChunkedBody = ByteString("3\r\nfoo\r\n1WHATTT\r\n12345678901\r\n0\r\n\r\n")


  def foldTester[A](p: Pipe[A, DataBuffer]): PipeFoldTester[ByteString] = new PipeFoldTester(p.fold(ByteString())(PipeFoldTester.byteFolder))

  "ChunkPassthroughPipe" must {

    "leave the data untouched" in {
      val p = new ChunkPassThroughPipe
      val c = foldTester(p)
      p.push(DataBuffer(chunkedBody)) must equal(PushResult.Complete)
      c.expect(chunkedBody)
    }

    "properly detect the end of the stream" in {
      val p = new ChunkPassThroughPipe
      val c = foldTester(p)

      val extra = ByteString("ADSFASDFASDF")
      val data = DataBuffer(chunkedBody ++ extra)
      p.push(data) must equal(PushResult.Complete)
      c.expect(chunkedBody)
      data.remaining must equal(extra.size)

    }

    "terminate the pipe on parsing exception" in {
      val p = new ChunkPassThroughPipe
      val c = foldTester(p)
      p.push(DataBuffer(badChunkedBody)) mustBe a[PushResult.Error]
      c.expectFailure()
    }


  }

  "ChunkDecodingPipe" must {

    "Process a chunked body" in {
      val expected = ByteString("foo12345678901")
      val p = new ChunkDecodingPipe
      val c = foldTester(p)
      p.push(DataBuffer(chunkedBody)) must equal(PushResult.Complete)
      c.expect(expected)
    }


  }


}
