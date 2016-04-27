package colossus.metrics

import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.duration._

//has to be a better way
object ConfigHelpers {

  implicit class ConfigExtractors(config : Config) {

    import scala.collection.JavaConversions._

    def getStringOption(path : String) : Option[String] = getOption(path, config.getString)

    def getIntOption(path : String) : Option[Int] = getOption(path, config.getInt)

    def getLongOption(path : String) : Option[Long] = getOption(path, config.getLong)

    private def getOption[T](path : String, f : String => T) : Option[T] = {
      if(config.hasPath(path)){
        Some(f(path))
      }else{
        None
      }
    }

    def getFiniteDurations(path : String) : Seq[FiniteDuration] = config.getStringList(path).map(finiteDurationOnly(_, path))

    def getFiniteDuration(path : String) : FiniteDuration = finiteDurationOnly(config.getString(path), path)

    def getFiniteDurationOption(path: String) : Option[FiniteDuration] = getOption(path, getFiniteDuration)

    def getScalaDuration(path : String) : Duration =  Duration(config.getString(path))

    private def finiteDurationOnly(str : String, key : String) = {
      Duration(str) match {
        case duration : FiniteDuration => duration
        case other => throw new FiniteDurationExpectedException(s"$str is not a valid FiniteDuration.  Expecting only finite for path $key.  Evaluted to $other")
      }
    }

    def withFallbacks(paths : String*) : Config = {
      //starting from empty, walk back from the lowest priority, stacking higher priorities on top of it.
      paths.reverse.foldLeft(ConfigFactory.empty()) {
        case (acc, path) =>if(config.hasPath(path)){
          config.getConfig(path).withFallback(acc)
        } else{
          acc
        }
      }
    }
  }
}

private[metrics] class FiniteDurationExpectedException(str : String) extends Exception(str)
