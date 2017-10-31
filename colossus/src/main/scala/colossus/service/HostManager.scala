package colossus.service

import scala.concurrent.duration._

sealed trait LBBackoff

case class LinearBackoff(
    pause: FiniteDuration = 100.milliseconds
) extends LBBackoff

case class ExponentialBackoff(
    initial: FiniteDuration = 1.milliseconds,
    max: FiniteDuration = 1.second
) extends LBBackoff

sealed trait LBReplacement
case object Random             extends LBReplacement
case object WithoutReplacement extends LBReplacement

sealed trait LBAttempts
case class MaxAttempts(max: Int)                     extends LBAttempts
case class EveryHost(upperLimit: Int = Int.MaxValue) extends LBAttempts

case class LBRetryPolicy(
    replacement: LBReplacement = WithoutReplacement,
    backoff: LBBackoff = ExponentialBackoff(),
    attempts: LBAttempts = EveryHost()
)

case class HostManager(retryPolicy: LBRetryPolicy = LBRetryPolicy()) {

  private var hosts                = Seq.empty[(String, Int)]
  private var loadBalancingClients = Seq.empty[LBC]

  def addHost(host: String, port: Int): Unit = {
    hosts = (host, port) +: hosts

    loadBalancingClients.foreach(_.addClient(host, port))
  }

  def removeHost(host: String, port: Int, allInstances: Boolean = true): Unit = {
    val pair = (host, port)
    hosts = if (allInstances) {
      hosts.filterNot(_ == pair)
    } else {
      dropFirstMatch(hosts, pair)
    }

    loadBalancingClients.foreach(_.removeClient(host, port, allInstances))
  }

  private def dropFirstMatch[A](ls: Seq[A], value: A): Seq[A] = {
    val index = ls.indexOf(value) //index is -1 if there is no match
    if (index < 0) {
      ls
    } else if (index == 0) {
      ls.tail
    } else {
      // splitAt keeps the matching element in the second group
      val (a, b) = ls.splitAt(index)
      a ++ b.tail
    }
  }

  private[service] def attachLoadBalancer(loadBalanceClient: LBC): Unit = {
    loadBalancingClients = loadBalanceClient +: loadBalancingClients

    hosts.foreach { case (host, port) => loadBalanceClient.addClient(host, port) }
  }
}
