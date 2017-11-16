package colossus.service

import colossus.core.RetryPolicy
import com.typesafe.config.Config

case class LoadBalancerSettings(retryPolicy: RetryPolicy, selection: SelectionPolicy)

object LoadBalancerSettings {
  def apply(config: Config): LoadBalancerSettings = {
    LoadBalancerSettings(
      RetryPolicy.fromConfig(config.getConfig("retry-policy")),
      SelectionPolicy.fromConfig(config.getConfig("selection-policy"))
    )
  }
}

sealed trait SelectionPolicy
case class RandomHosts(limit: Int)         extends SelectionPolicy
case class UniqueHosts(limit: Option[Int]) extends SelectionPolicy

object SelectionPolicy {
  def fromConfig(config: Config): SelectionPolicy = {
    val selectionPolicyType = config.getString("type").toUpperCase

    selectionPolicyType match {
      case "RANDOM" =>
        RandomHosts(config.getInt("limit"))
      case "UNIQUE" =>
        if (config.hasPath("limit")) {
          UniqueHosts(Some(config.getInt("limit")))
        } else {
          UniqueHosts(None)
        }
      case _ => throw new Exception(s"Unknown SelectionPolicy $selectionPolicyType")
    }
  }
}
