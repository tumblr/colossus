package colossus.metrics

import com.typesafe.config.Config

private[metrics] trait CollectorConfigLoader {

  /**
    * Build a Config by stacking config objects specified by the paths elements
    *
    * @param config
    * @param address Address of the Metric, turned into a path and used as a config source.
    * @param paths Ordered list of config paths, ordered by highest precedence.
    * @return
    */
  def resolveConfig(config : Config, address : MetricAddress, paths : String*) : Config = {
    import ConfigHelpers._
    val p = address.pieceString.replace('/','.') +: paths
    config.withFallbacks(p : _*)
  }
}
