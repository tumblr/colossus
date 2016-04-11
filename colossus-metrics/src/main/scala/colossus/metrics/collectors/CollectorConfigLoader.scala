package colossus.metrics

import com.typesafe.config.{ConfigFactory, Config}

private[metrics] trait CollectorConfigLoader {

  /**
    * Build a Config by stacking config objects specified by the paths elements
    * @param config
    * @param paths Ordered list of config paths, ordered by highest precedence.
    * @return
    */
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
