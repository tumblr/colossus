package colossus.service

trait RequestFormatter[I] {

  /**
    * None means do not log.
    * Some(false) means log only the name.
    * Some(true) means log with stack trace (default).
    */
  def logWithStackTrace(error: Throwable): Option[Boolean]

  def format(request: I, error: Throwable): Option[String]

  final def formatLogMessage(request: I, error: Throwable): Option[RequestFormatter.LogMessage] = {
    logWithStackTrace(error).map { includeStackTrace =>
      val message = format(request, error).getOrElse(s"${error.getClass.getSimpleName}: ${request.toString}")
      RequestFormatter.LogMessage(message, includeStackTrace)
    }
  }
}

object RequestFormatter {
  case class LogMessage(message: String, includeStackTrace: Boolean)
}
