package colossus.streaming

/**
  * This is the base type of both [[Source]] and [[Sink]]
  */
trait Transport {

  /**
    * Immediately terminate the transport, permenantly putting it into an error state
    */
  def terminate(reason: Throwable)

}

sealed trait TransportState
sealed trait NonOpenTransportState extends TransportState
object TransportState {
  case object Open                         extends TransportState
  case object Closed                       extends NonOpenTransportState
  case class Terminated(reason: Throwable) extends NonOpenTransportState
}
