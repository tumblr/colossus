package colossus.metrics

import com.typesafe.config.{ConfigFactory, Config}

private[metrics] trait CollectorConfigLoader {

  /**
    * Build a Config by stacking config objects specified by the paths elements
    *
    * @param config
    * @param paths Ordered list of config paths, ordered by highest precedence.
    * @return
    */
  def resolveConfig(config : Config, paths : String*) : Config = {
    //starting from empty, walk back from the lowest priority, stacking higher priorities on top of it.
    paths.reverse.foldLeft(ConfigFactory.empty()) {
      case (acc, path) =>if(config.hasPath(path)){
        config.getConfig(path).withFallback(acc)
      } else{
        acc
      }
    }
  }

  def resolveConfig(fullAddress : MetricAddress, msConfig : Config,  externalConfig: Option[Config], paths : String*) : Config = {
    val addressPath = fullAddress.pieceString.replace('/','.')
    val c = externalConfig.getOrElse(msConfig)
    resolveConfig(c,(addressPath +: paths):_*)
  }
}
