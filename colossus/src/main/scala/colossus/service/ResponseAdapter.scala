package colossus.service

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds



trait Sender[C <: CodecDSL, M[_]] {

  def send(input: C#Input): M[C#Output]

}

trait ResponseAdapter[C <: CodecDSL, M[_]] extends Sender[C,M] {

  protected def client : Sender[C, M]

  def send(i : C#Input) : M[C#Output] = client.send(i)

  protected def executeAndMap[T](i : C#Input)(f : C#Output => M[T]) = flatMap(send(i))(f)

  protected def map[T, U](t : M[T])(f : T => U)  : M[U]

  protected def flatMap[T](t : M[C#Output])(f : C#Output => M[T]) : M[T]

  protected def success[T](t : T) : M[T]

  protected def failure[T](ex : Throwable) : M[T]
}

trait CallbackResponseAdapter[C <: CodecDSL] extends ResponseAdapter[C, Callback] {

  override protected def map[T, U](t: Callback[T])(f: (T) => U): Callback[U] = t.map(f)

  override protected def flatMap[T](t: Callback[C#Output])(f: (C#Output) => Callback[T]): Callback[T] = t.flatMap(f)

  override protected def success[T](t: T): Callback[T] = Callback.successful(t)

  override protected def failure[T](ex: Throwable): Callback[T] = Callback.failed(ex)
}

trait FutureResponseAdapter[C <: CodecDSL] extends ResponseAdapter[C, Future] {

  implicit protected def executionContext : ExecutionContext

  override protected def map[T, U](t: Future[T])(f: (T) => U): Future[U] = t.map(f)

  override protected def flatMap[T](t: Future[C#Output])(f: (C#Output) => Future[T]): Future[T] = t.flatMap(f)

  override protected def success[T](t: T): Future[T] = Future.successful(t)

  override protected def failure[T](ex: Throwable): Future[T] = Future.failed(ex)
}
