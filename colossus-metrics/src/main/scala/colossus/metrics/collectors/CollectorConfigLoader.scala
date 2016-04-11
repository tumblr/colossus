package colossus.metrics

import com.typesafe.config.{ConfigFactory, Config}

private[metrics] trait CollectorConfigLoader {

  def resolveConfig(config : Config, paths : String*) : Config = {
    paths.reverse.foldLeft(ConfigFactory.empty()) {
      case (acc, path) =>if(config.hasPath(path)){
        config.getConfig(path).withFallback(acc)
      } else{
        acc
      }
    }
  }
}
