package colossus.streaming

sealed trait PushResult
object PushResult {

  //the item was successfully pushed
  case object Ok extends PushResult

  //the item was not pushed because the Sink is already full.  The returned
  //[[Signal]] can be used to be notified when the sink can accept more items
  case class Full(onReady: Signal) extends PushResult

  //The Sink has been manually closed (without error) and is not accepting any more items
  case object Closed extends PushResult

  //The Sink has been terminated and cannot accept more items
  case class Error(reason: Throwable) extends PushResult
}
