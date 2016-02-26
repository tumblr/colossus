package colossus
package protocols.http

import akka.util.ByteString

/**
 * This trait mixed in to any codes that do not allow a body in the response
 */
trait NoBodyCode

case class HttpCode(code: Int, description: String) {
  val headerSegment = s"$code $description"
  val headerBytes = ByteString(headerSegment)
  val headerArr = headerBytes.toArray
}
object HttpCode {
  def apply(code: Int): HttpCode = HttpCodes(code)
}

object HttpCodes {
  //100
  val CONTINUE              = new HttpCode(100, "Continue") with NoBodyCode
  val SWITCHING_PROTOCOLS   = new HttpCode(101, "Switching Protocols") with NoBodyCode
  val PROCESSING            = new HttpCode(102, "Processing") with NoBodyCode
  //200
  val OK = HttpCode(200, "OK")
  val CREATED = HttpCode(201, "Created")
  val ACCEPTED = HttpCode(202, "Accepted")
  val NON_AUTHORITATIVE_INFORMATION = HttpCode(203, "Non-Authoritative Information")
  val NO_CONTENT = new HttpCode(204, "No Content") with NoBodyCode
  val RESET_CONTENT = HttpCode(205, "Reset Content")
  val PARTIAL_CONTENT = HttpCode(206, "Partial Content")
  val MULTI_STATUS = HttpCode(207, "Multi-Status")
  val ALREADY_REPORTED = HttpCode(208, "Already Reported")
  val IM_USED = HttpCode(226, "IM Used")
  //300
  val MULTIPLE_CHOICES = HttpCode(300, "Multiple Choices")
  val MOVED_PERMANENTLY = HttpCode(301, "Moved Permanently")
  val FOUND = HttpCode(302, "Found")
  val SEE_OTHER = HttpCode(303, "See Other")
  val NOT_MODIFIED = new HttpCode(304, "Not Modified") with NoBodyCode
  val USE_PROXY = HttpCode(305, "Use Proxy")
  val TEMPORARY_REDIRECT = HttpCode(307, "Temporary Redirect")
  val PERMANENT_REDIRECT = HttpCode(308, "Permanent Redirect")
  //400
  val BAD_REQUEST = HttpCode(400, "Bad Request")
  val UNAUTHORIZED = HttpCode(401, "Unauthorized")
  val PAYMENT_REQUIRED = HttpCode(402, "Payment Required")
  val FORBIDDEN = HttpCode(403, "Forbidden")
  val NOT_FOUND = HttpCode(404, "Not Found")
  val METHOD_NOT_ALLOWED = HttpCode(405, "Method Not Allowed")
  val NOT_ACCEPTABLE = HttpCode(406, "Not Acceptable")
  val PROXY_AUTHENTICATION_REQUIRED = HttpCode(407, "Proxy Authentication Required")
  val REQUEST_TIMEOUT = HttpCode(408, "Request Timeout")
  val CONFLICT = HttpCode(409, "Conflict")
  val GONE = HttpCode(410, "Gone")
  val LENGTH_REQUIRED = HttpCode(411, "Length Required")
  val PRECONDITION_FAILED = HttpCode(412, "Precondition Failed")
  val PAYLOAD_TOO_LARGE = HttpCode(413, "Payload Too Large")
  val URI_TOO_LONG = HttpCode(414, "URI Too Long")
  val UNSUPPORTED_MEDIA_TYPE = HttpCode(415, "Unsupported Media Type")
  val REQUESTED_RANGE_NOT_SATISFIABLE = HttpCode(416, "Requested Range Not Satisfiable")
  val EXPECTATION_FAILED = HttpCode(417, "Expectation Failed")
  val UNPROCESSABLE_ENTITY = HttpCode(422, "Unprocessable Entity")
  val LOCKED = HttpCode(423, "Locked")
  val FAILED_DEPENDENCY = HttpCode(424, "Failed Dependency")
  val UPGRADE_REQUIRED = HttpCode(426, "Upgrade Required")
  val PRECONDITION_REQUIRED = HttpCode(428, "Precondition Required")
  val TOO_MANY_REQUESTS = HttpCode(429, "Too Many Requests")
  val REQUEST_HEADER_FIELDS_TOO_LARGE = HttpCode(431, "Request Header Fields Too Large")
  //500
  val INTERNAL_SERVER_ERROR = HttpCode(500, "Internal Server Error")
  val NOT_IMPLEMENTED = HttpCode(501, "Not Implemented")
  val BAD_GATEWAY = HttpCode(502, "Bad Gateway")
  val SERVICE_UNAVAILABLE = HttpCode(503, "Service Unavailable")
  val GATEWAY_TIMEOUT = HttpCode(504, "Gateway Timeout")
  val HTTP_VERSION_NOT_SUPPORTED = HttpCode(505, "HTTP Version Not Supported")
  val VARIANT_ALSO_NEGOTIATES = HttpCode(506, "Variant Also Negotiates (Experimental)")
  val INSUFFICIENT_STORAGE = HttpCode(507, "Insufficient Storage")
  val LOOP_DETECTED = HttpCode(508, "Loop Detected")
  val NOT_EXTENDED = HttpCode(510, "Not Extended")
  val NETWORK_AUTHENTICATION_REQUIRED = HttpCode(511, "Network Authentication Required")

  def apply(code : Int) = {
    code match {
      case CONTINUE.code => CONTINUE
      case SWITCHING_PROTOCOLS.code => SWITCHING_PROTOCOLS
      case PROCESSING.code => PROCESSING
      case OK.code => OK
      case CREATED.code => CREATED
      case ACCEPTED.code => ACCEPTED
      case NON_AUTHORITATIVE_INFORMATION.code => NON_AUTHORITATIVE_INFORMATION
      case NO_CONTENT.code => NO_CONTENT
      case RESET_CONTENT.code => RESET_CONTENT
      case PARTIAL_CONTENT.code => PARTIAL_CONTENT
      case MULTI_STATUS.code => MULTI_STATUS
      case ALREADY_REPORTED.code => ALREADY_REPORTED
      case IM_USED.code => IM_USED
      case MULTIPLE_CHOICES.code => MULTIPLE_CHOICES
      case MOVED_PERMANENTLY.code => MOVED_PERMANENTLY
      case FOUND.code => FOUND
      case SEE_OTHER.code => SEE_OTHER
      case NOT_MODIFIED.code => NOT_MODIFIED
      case USE_PROXY.code => USE_PROXY
      case TEMPORARY_REDIRECT.code => TEMPORARY_REDIRECT
      case PERMANENT_REDIRECT.code => PERMANENT_REDIRECT
      case BAD_REQUEST.code => BAD_REQUEST
      case UNAUTHORIZED.code => UNAUTHORIZED
      case PAYMENT_REQUIRED.code => PAYMENT_REQUIRED
      case FORBIDDEN.code => FORBIDDEN
      case NOT_FOUND.code => NOT_FOUND
      case METHOD_NOT_ALLOWED.code => METHOD_NOT_ALLOWED
      case NOT_ACCEPTABLE.code => NOT_ACCEPTABLE
      case PROXY_AUTHENTICATION_REQUIRED.code => PROXY_AUTHENTICATION_REQUIRED
      case REQUEST_TIMEOUT.code => REQUEST_TIMEOUT
      case CONFLICT.code => CONFLICT
      case GONE.code => GONE
      case LENGTH_REQUIRED.code => LENGTH_REQUIRED
      case PRECONDITION_FAILED.code => PRECONDITION_FAILED
      case PAYLOAD_TOO_LARGE.code => PAYLOAD_TOO_LARGE
      case URI_TOO_LONG.code => URI_TOO_LONG
      case UNSUPPORTED_MEDIA_TYPE.code => UNSUPPORTED_MEDIA_TYPE
      case REQUESTED_RANGE_NOT_SATISFIABLE.code => REQUESTED_RANGE_NOT_SATISFIABLE
      case EXPECTATION_FAILED.code => EXPECTATION_FAILED
      case UNPROCESSABLE_ENTITY.code => UNPROCESSABLE_ENTITY
      case LOCKED.code => LOCKED
      case FAILED_DEPENDENCY.code => FAILED_DEPENDENCY
      case UPGRADE_REQUIRED.code => UPGRADE_REQUIRED
      case PRECONDITION_REQUIRED.code => PRECONDITION_REQUIRED
      case TOO_MANY_REQUESTS.code => TOO_MANY_REQUESTS
      case REQUEST_HEADER_FIELDS_TOO_LARGE.code => REQUEST_HEADER_FIELDS_TOO_LARGE
      case INTERNAL_SERVER_ERROR.code => INTERNAL_SERVER_ERROR
      case NOT_IMPLEMENTED.code => NOT_IMPLEMENTED
      case BAD_GATEWAY.code => BAD_GATEWAY
      case SERVICE_UNAVAILABLE.code => SERVICE_UNAVAILABLE
      case GATEWAY_TIMEOUT.code => GATEWAY_TIMEOUT
      case HTTP_VERSION_NOT_SUPPORTED.code => HTTP_VERSION_NOT_SUPPORTED
      case VARIANT_ALSO_NEGOTIATES.code => VARIANT_ALSO_NEGOTIATES
      case INSUFFICIENT_STORAGE.code => INSUFFICIENT_STORAGE
      case LOOP_DETECTED.code => LOOP_DETECTED
      case NOT_EXTENDED.code => NOT_EXTENDED
      case NETWORK_AUTHENTICATION_REQUIRED.code => NETWORK_AUTHENTICATION_REQUIRED
      case _ => throw new Exception(s"Unsupported HTTP status: $code")
    }
  }
}
