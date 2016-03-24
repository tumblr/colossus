package colossus.metrics

import com.typesafe.config.Config

private[metrics] trait CollectorConfigLoader {

  def resolveConfig(config : Config,
                    metricPath : String,
                    defaultCollectorPath : String) : Config = {
    if(config.hasPath(metricPath)) {
      config.getConfig(metricPath).withFallback(config.getConfig(defaultCollectorPath))
    }else{
      config.getConfig(defaultCollectorPath)
    }

  }
}
