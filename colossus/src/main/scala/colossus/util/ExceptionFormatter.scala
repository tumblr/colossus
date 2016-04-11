package colossus
package util



object ExceptionFormatter {

  implicit class ExceptionFormatterOps(val exception: Throwable) extends AnyVal {

    def metricsName: String = exception.getClass.getName.replaceAll("[^\\w]", "")

  }

}
