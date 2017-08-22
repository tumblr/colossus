package colossus.streaming

sealed trait StreamComponent
object StreamComponent {
  case object Head extends StreamComponent
  case object Body extends StreamComponent
  case object Tail extends StreamComponent
}

trait Stream[T] {

  def component(t: T): StreamComponent

}
