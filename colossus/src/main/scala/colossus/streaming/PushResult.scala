package colossus.streaming


sealed trait PushResult {
  def pushed: Boolean
}
object PushResult {
  sealed trait Pushed extends PushResult {
    def pushed = true
  }
  sealed trait NotPushed extends PushResult {
    def pushed = false
  }

  //the item was successfully pushed and is ready for more data
  case object Ok extends Pushed

  //the item was successfully pushed but the pipe is not yet ready for more data, trigger is called when it's ready
  case class Filled(onReady: Signal) extends Pushed

  //the item was not pushed because the pipe is already full, the same trigger
  //returned when the Filled result was returned is included
  case class Full(onReady: Signal) extends NotPushed

  //The pipe has been manually closed (without error) and is not accepting any more items
  case object Closed extends NotPushed

  //The pipe has been terminated or some other error has occurred
  case class Error(reason: Throwable) extends NotPushed
}
