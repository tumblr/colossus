package colossus.protocols.http.client



import akka.util.ByteString
import colossus.protocols.http.{Http, HttpBody, HttpHeaders, HttpMethod, HttpRequest, HttpRequestHead, HttpResponse}
import colossus.protocols.http.filters.HttpCustomFilters
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler
import colossus.util.{GzipCompressor, ZCompressor}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{MustMatchers, WordSpec}

class HttpCustomFiltersSpec extends WordSpec with MustMatchers with ScalaFutures {
  import scala.concurrent.ExecutionContext.Implicits.global
  val helloWorldPartialHandler: PartialHandler[Http] = { case request =>
    Callback.successful(request.ok("hello-world"))
  }

  "Gzip custom filter" should {
    "do not compress" in {
      val filter = new HttpCustomFilters.CompressionFilter()
      val e: Callback[HttpResponse] = filter.apply(helloWorldPartialHandler)(HttpRequest.base)
      e.toFuture.futureValue.body.bytes mustBe(ByteString("hello-world"))
    }

    "compress if accept encoding is deflate" in {
      val filter = new HttpCustomFilters.CompressionFilter()
      val request = HttpRequest(
        HttpMethod.Get,
        "/",
        HttpHeaders() + (HttpHeaders.AcceptEncoding, "deflate"),
        HttpBody.NoBody)

      val c = new ZCompressor()
      val e: Callback[HttpResponse] = filter.apply(helloWorldPartialHandler)(request)

      e.toFuture.futureValue.body.bytes mustBe(c.compress(ByteString("hello-world".getBytes())))
    }

    "won't compress if accept encoding is no supported" in {
      val filter = new HttpCustomFilters.CompressionFilter()
      val request = HttpRequest(
        HttpMethod.Get,
        "/",
        HttpHeaders() + (HttpHeaders.AcceptEncoding, "compress"),
        HttpBody.NoBody)

      val e: Callback[HttpResponse] = filter.apply(helloWorldPartialHandler)(request)

      e.toFuture.futureValue.body.bytes mustBe(ByteString("hello-world".getBytes()))
    }


    "compress if accept encoding is gzip" in {
      val filter = new HttpCustomFilters.CompressionFilter()
      val request = HttpRequest(
        HttpMethod.Get,
        "/",
        HttpHeaders() + (HttpHeaders.AcceptEncoding, "gzip"),
        HttpBody.NoBody)

      val c = new GzipCompressor()
      val e: Callback[HttpResponse] = filter.apply(helloWorldPartialHandler)(request)

      e.toFuture.futureValue.body.bytes mustBe(c.compress(ByteString("hello-world".getBytes())))
    }
  }
}
