package colossus.streaming


sealed trait PullResult[+T]
sealed trait NEPullResult[+T] extends PullResult[T]
object PullResult {
  case class Item[T](item: T) extends NEPullResult[T]
  case class Empty(whenReady: Signal) extends PullResult[Nothing]
  case object Closed extends NEPullResult[Nothing]
  case class Error(reason: Throwable) extends NEPullResult[Nothing]

  implicit object NEPullResultMapper extends Functor[NEPullResult] {
    def map[A,B](p: NEPullResult[A], f: A => B): NEPullResult[B] = p match {
      case Item(i)  => Item(f(i))
      case Closed   => Closed
      case Error(r) => Error(r)
    }
  }
  implicit object PullResultMapper extends Functor[PullResult] {
    def map[A,B](p: PullResult[A], f: A => B): PullResult[B] = p match {
      case Empty(signal) => Empty(signal)
      case Item(i)    => Item(f(i))
      case Closed     => Closed
      case Error(r)   => Error(r)
    }
  }
}
