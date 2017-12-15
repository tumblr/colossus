package colossus.service

/**
  * Possible ways that an exception can be logged
  */
sealed trait RequestFormatType

object RequestFormatType {

  case object DoNotLog extends RequestFormatType

  case object LogNameOnly extends RequestFormatType

  case object LogWithStackTrace extends RequestFormatType
}

/**
  * A request formatter is used to determine the message that is logged when a request fails.
  *
  * @tparam I Protocol request
  */
trait RequestFormatter[I] {

  def formatterOption(error: Throwable): RequestFormatType

  def format(request: Option[I], error: Throwable): String = {
    s"${error.getClass.getSimpleName}: ${request.getOrElse("Invalid request")}"
  }

  final def formatLogMessage(request: Option[I], error: Throwable): Option[RequestFormatter.LogMessage] = {
    formatterOption(error) match {
      case RequestFormatType.DoNotLog =>
        None
      case other =>
        Some(RequestFormatter.LogMessage(format(request, error), other == RequestFormatType.LogWithStackTrace))
    }
  }
}

object RequestFormatter {
  case class LogMessage(message: String, includeStackTrace: Boolean)
}
