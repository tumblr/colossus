package colossus

import scala.language.higherKinds

package object streaming {

  trait Functor[F[_]] {
    def map[A, B](a: F[A], f: A => B): F[B]
  }

  implicit class FunctorOps[F[_], A](val value: F[A]) extends AnyVal {
    def map[B](f: A => B)(implicit fct: Functor[F]): F[B] = fct.map(value, f)
  }

  implicit object SourceMapper extends Functor[Source] {
    def map[A, B](source: Source[A], fn: A => B): Source[B] = new Source[B] {
      def pull(): PullResult[B] = source.pull().map(fn)
      def peek                  = source.peek.map(fn)

      def outputState = source.outputState
      def terminate(err: Throwable) {
        source.terminate(err)
      }
      override def pullWhile(whilefn: B => PullAction, onc: TerminalPullResult => Any) {
        source.pullWhile(x => whilefn(fn(x)), onc)
      }
    }

  }

  implicit object SourceFilterMapper {

    def filterMap[A, B](source: Source[A], fn: A => Option[B]): Source[B] = new Source[B] {
      def pull(): PullResult[B] = source.pull().map(fn) match {
        case PullResult.Item(Some(item)) => PullResult.Item(item)
        case PullResult.Item(None)       => pull()
        case other                       => other.asInstanceOf[PullResult[B]]
      }

      def peek = source.peek.map(fn) match {
        case PullResult.Item(Some(item)) => PullResult.Item(item)
        case PullResult.Item(None) => {
          source.pull()
          peek
        }
        case other => other.asInstanceOf[PullResult[B]]
      }
      def outputState = source.outputState
      def terminate(err: Throwable) {
        source.terminate(err)
      }
      override def pullWhile(whilefn: B => PullAction, onc: TerminalPullResult => Any) {
        source.pullWhile(x =>
                           fn(x) match {
                             case Some(item) => whilefn(item)
                             case None       => PullAction.PullContinue
                         },
                         onc)
      }
    }
  }

  implicit class SourceOps[A](val source: Source[A]) extends AnyVal {
    def filterMap[B](f: A => Option[B]): Source[B] = SourceFilterMapper.filterMap(source, f)
  }

  //note - sadly trying to unify this with a HKT like Functor doesn't seem to
  //work since type inferrence fails on the type-lambda needed to squash
  //Pipe[_,_] down to M[_].  See: https://issues.scala-lang.org/browse/SI-6895

  implicit class PipeOps[A, B](val pipe: Pipe[A, B]) extends AnyVal {
    def map[C](fn: B => C): Pipe[A, C] = {
      val mappedsource = SourceMapper.map(pipe, fn)
      new Channel(pipe, mappedsource)
    }

    def filterMap[C](fn: B => Option[C]): Pipe[A, C] = {
      val mappedsource = SourceFilterMapper.filterMap(pipe, fn)
      new Channel(pipe, mappedsource)
    }
  }

}
