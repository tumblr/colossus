package colossus.metrics.logging

import org.slf4j.{Logger, LoggerFactory}

trait ColossusLogging {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def debug(s: => String): Unit = {
    doLog(s, log.isDebugEnabled, log.debug)
  }

  def warn(s: => String): Unit = {
    doLog(s, log.isWarnEnabled, log.warn)
  }

  def info(s: => String): Unit = {
    doLog(s, log.isInfoEnabled, log.info)
  }

  def trace(s: => String): Unit = {
    doLog(s, log.isTraceEnabled, log.trace)
  }

  def error(s: => String): Unit = {
    doLog(s, log.isErrorEnabled, log.error)
  }

  def error(s: => String, t: Throwable): Unit = {
    doLog(s, log.isErrorEnabled, log.error(_: String, t))
  }

  def formatIterable(iterable: Iterable[Any], size: Int = 10): String = {
    if (iterable.size > size) {
      s"${iterable.take(size)}..."
    } else {
      iterable.toString()
    }
  }

  private def doLog(s: => String, writeEnabled: Boolean, write: String => Unit): Unit = {
    if (writeEnabled) {
      write(s)
    }
  }
}
