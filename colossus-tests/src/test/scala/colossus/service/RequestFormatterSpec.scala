package colossus.service

import colossus.protocols.http.HttpRequest
import org.scalatest.WordSpec

class RequestFormatterSpec extends WordSpec {
  "Request formatter" must {
    "format log message with exception name, request and stack trace when no special format implementation provided" in {
      val requestFormatter = new RequestFormatter[HttpRequest] {
        override def formatterOption(error: Throwable): RequestFormatType = RequestFormatType.LogWithStackTrace
      }

      val output = requestFormatter.formatLogMessage(Some(HttpRequest.get("/")), new Exception("Too fast"))

      val expected = RequestFormatter.LogMessage(
        "Exception: HttpRequest(BuiltHead(Get / 1.1,[]),)",
        includeStackTrace = true
      )

      assert(output.contains(expected))
    }

    "format log message with exception name and request when no special format implementation provided" in {
      val requestFormatter = new RequestFormatter[HttpRequest] {
        override def formatterOption(error: Throwable): RequestFormatType = RequestFormatType.LogNameOnly
      }

      val output = requestFormatter.formatLogMessage(Some(HttpRequest.get("/")), new RuntimeException("No running"))

      val expected = RequestFormatter.LogMessage(
        "RuntimeException: HttpRequest(BuiltHead(Get / 1.1,[]),)",
        includeStackTrace = false
      )

      assert(output.contains(expected))
    }

    "return nothing if logging is turned off" in {
      val requestFormatter = new RequestFormatter[HttpRequest] {
        override def formatterOption(error: Throwable): RequestFormatType = RequestFormatType.DoNotLog
      }

      val output = requestFormatter.formatLogMessage(Some(HttpRequest.get("/")), new RuntimeException("No running"))

      assert(output.isEmpty)
    }
  }

}
